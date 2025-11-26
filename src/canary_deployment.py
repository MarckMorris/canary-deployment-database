#!/usr/bin/env python3
"""
Canary Deployment System for Database Changes
Progressive rollout with automated rollback on errors
"""

import psycopg2
import time
import random
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)


class CanaryDeployment:
    
    def __init__(self):
        self.stable_conn = None
        self.canary_conn = None
        self.canary_percentage = 0
        self.metrics = {'stable': [], 'canary': []}
        
    def connect_all(self):
        try:
            self.stable_conn = psycopg2.connect(
                host='localhost', port=5454,
                dbname='stable_db', user='postgres', password='postgres'
            )
            self.stable_conn.autocommit = True
            
            self.canary_conn = psycopg2.connect(
                host='localhost', port=5455,
                dbname='canary_db', user='postgres', password='postgres'
            )
            self.canary_conn.autocommit = True
            
            logger.info("Connected to stable and canary databases")
            return True
        except Exception as e:
            logger.error(f"Connection failed: {e}")
            return False
    
    def setup(self):
        """Setup both databases"""
        
        for conn, env in [(self.stable_conn, 'stable'), (self.canary_conn, 'canary')]:
            cursor = conn.cursor()
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    user_id SERIAL PRIMARY KEY,
                    username VARCHAR(100),
                    email VARCHAR(100),
                    created_at TIMESTAMP DEFAULT NOW()
                );
                
                CREATE TABLE IF NOT EXISTS deployment_metrics (
                    metric_id SERIAL PRIMARY KEY,
                    timestamp TIMESTAMP DEFAULT NOW(),
                    environment VARCHAR(20),
                    query_latency_ms DECIMAL(10,2),
                    error_rate DECIMAL(5,2),
                    success_count INT,
                    error_count INT
                );
                
                INSERT INTO users (username, email)
                SELECT 'user_' || i, 'user' || i || '@test.com'
                FROM generate_series(1, 100) i
                ON CONFLICT DO NOTHING;
            """)
            cursor.close()
        
        logger.info("Databases initialized")
    
    def apply_canary_change(self):
        """Apply the new change to canary database"""
        
        logger.info("Applying new schema change to CANARY...")
        
        cursor = self.canary_conn.cursor()
        
        # Simulated change: Add index for performance optimization
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_users_email_optimized 
            ON users(email, username);
        """)
        
        cursor.close()
        logger.info("✓ Canary change applied")
    
    def route_traffic(self, percentage: int):
        """Set percentage of traffic going to canary"""
        self.canary_percentage = percentage
        logger.info(f"Traffic routing: {100-percentage}% stable, {percentage}% canary")
    
    def execute_query(self, query: str, use_canary: bool = False):
        """Execute query on stable or canary"""
        
        conn = self.canary_conn if use_canary else self.stable_conn
        env = 'canary' if use_canary else 'stable'
        
        cursor = conn.cursor()
        
        start_time = time.time()
        success = True
        
        try:
            cursor.execute(query)
            cursor.fetchall()
            latency = (time.time() - start_time) * 1000
            
            # Record metric
            cursor.execute("""
                INSERT INTO deployment_metrics 
                (environment, query_latency_ms, error_rate, success_count, error_count)
                VALUES (%s, %s, %s, %s, %s)
            """, (env, latency, 0.0, 1, 0))
            
        except Exception as e:
            latency = (time.time() - start_time) * 1000
            success = False
            
            cursor.execute("""
                INSERT INTO deployment_metrics 
                (environment, query_latency_ms, error_rate, success_count, error_count)
                VALUES (%s, %s, %s, %s, %s)
            """, (env, latency, 100.0, 0, 1))
        
        cursor.close()
        
        return success, latency
    
    def simulate_traffic(self, total_requests: int = 100):
        """Simulate application traffic with canary routing"""
        
        logger.info(f"Simulating {total_requests} requests...")
        
        for i in range(total_requests):
            # Decide routing based on canary percentage
            use_canary = random.randint(1, 100) <= self.canary_percentage
            
            # Execute query
            query = "SELECT * FROM users WHERE email LIKE '%test.com' LIMIT 10"
            success, latency = self.execute_query(query, use_canary)
            
            env = 'canary' if use_canary else 'stable'
            self.metrics[env].append({
                'success': success,
                'latency': latency
            })
        
        logger.info(f"✓ Traffic simulation complete")
    
    def analyze_metrics(self) -> dict:
        """Analyze metrics from both environments"""
        
        analysis = {}
        
        for env in ['stable', 'canary']:
            if not self.metrics[env]:
                continue
            
            successes = sum(1 for m in self.metrics[env] if m['success'])
            total = len(self.metrics[env])
            error_rate = ((total - successes) / total * 100) if total > 0 else 0
            
            latencies = [m['latency'] for m in self.metrics[env]]
            avg_latency = sum(latencies) / len(latencies) if latencies else 0
            p95_latency = sorted(latencies)[int(len(latencies) * 0.95)] if latencies else 0
            
            analysis[env] = {
                'total_requests': total,
                'success_count': successes,
                'error_rate': error_rate,
                'avg_latency': avg_latency,
                'p95_latency': p95_latency
            }
        
        return analysis
    
    def should_rollback(self, analysis: dict) -> bool:
        """Determine if rollback is needed"""
        
        if 'canary' not in analysis or 'stable' not in analysis:
            return False
        
        canary = analysis['canary']
        stable = analysis['stable']
        
        # Rollback conditions
        if canary['error_rate'] > stable['error_rate'] * 2:
            logger.warning(f"⚠ Error rate spike: {canary['error_rate']:.2f}% vs {stable['error_rate']:.2f}%")
            return True
        
        if canary['p95_latency'] > stable['p95_latency'] * 1.5:
            logger.warning(f"⚠ Latency degradation: {canary['p95_latency']:.2f}ms vs {stable['p95_latency']:.2f}ms")
            return True
        
        return False
    
    def rollback_canary(self):
        """Rollback canary changes"""
        
        logger.info("Executing rollback on CANARY...")
        
        cursor = self.canary_conn.cursor()
        cursor.execute("DROP INDEX IF EXISTS idx_users_email_optimized")
        cursor.close()
        
        logger.info("✓ Rollback complete")
    
    def print_metrics(self, analysis: dict, phase: str):
        """Print metrics comparison"""
        
        print("\n" + "=" * 80)
        print(f"CANARY DEPLOYMENT METRICS - {phase}")
        print("=" * 80)
        print(f"Canary Traffic: {self.canary_percentage}%")
        
        for env in ['stable', 'canary']:
            if env not in analysis:
                continue
            
            metrics = analysis[env]
            
            print(f"\n{env.upper()} Environment:")
            print(f"  Total Requests: {metrics['total_requests']}")
            print(f"  Success Rate: {100 - metrics['error_rate']:.2f}%")
            print(f"  Error Rate: {metrics['error_rate']:.2f}%")
            print(f"  Avg Latency: {metrics['avg_latency']:.2f}ms")
            print(f"  P95 Latency: {metrics['p95_latency']:.2f}ms")
        
        # Comparison
        if 'stable' in analysis and 'canary' in analysis:
            stable = analysis['stable']
            canary = analysis['canary']
            
            latency_diff = ((canary['avg_latency'] - stable['avg_latency']) / stable['avg_latency'] * 100) if stable['avg_latency'] > 0 else 0
            error_diff = canary['error_rate'] - stable['error_rate']
            
            print(f"\nComparison:")
            print(f"  Latency Delta: {latency_diff:+.1f}%")
            print(f"  Error Rate Delta: {error_diff:+.2f}%")
        
        print("=" * 80)
    
    def run_canary_deployment(self):
        """Execute progressive canary deployment"""
        
        print("\n" + "=" * 80)
        print("CANARY DEPLOYMENT SYSTEM")
        print("=" * 80)
        
        if not self.connect_all():
            return
        
        self.setup()
        
        # Apply change to canary
        print("\nPHASE 1: Deploy to Canary")
        print("-" * 80)
        self.apply_canary_change()
        
        # Stage 1: 10% traffic
        print("\nPHASE 2: 10% Canary Traffic")
        print("-" * 80)
        self.route_traffic(10)
        self.simulate_traffic(200)
        
        analysis = self.analyze_metrics()
        self.print_metrics(analysis, "10% Traffic")
        
        if self.should_rollback(analysis):
            print("\n✗ DEPLOYMENT FAILED - Initiating Rollback")
            self.rollback_canary()
            return
        
        print("\n✓ 10% phase successful")
        time.sleep(2)
        
        # Stage 2: 50% traffic
        print("\nPHASE 3: 50% Canary Traffic")
        print("-" * 80)
        self.metrics = {'stable': [], 'canary': []}  # Reset
        self.route_traffic(50)
        self.simulate_traffic(200)
        
        analysis = self.analyze_metrics()
        self.print_metrics(analysis, "50% Traffic")
        
        if self.should_rollback(analysis):
            print("\n✗ DEPLOYMENT FAILED - Initiating Rollback")
            self.rollback_canary()
            return
        
        print("\n✓ 50% phase successful")
        time.sleep(2)
        
        # Stage 3: 100% traffic
        print("\nPHASE 4: 100% Canary Traffic (Full Rollout)")
        print("-" * 80)
        self.metrics = {'stable': [], 'canary': []}
        self.route_traffic(100)
        self.simulate_traffic(200)
        
        analysis = self.analyze_metrics()
        self.print_metrics(analysis, "100% Traffic")
        
        # Promote canary to stable
        print("\nPHASE 5: Promote Canary to Stable")
        print("-" * 80)
        logger.info("Applying change to stable environment...")
        
        cursor = self.stable_conn.cursor()
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_users_email_optimized 
            ON users(email, username);
        """)
        cursor.close()
        
        logger.info("✓ Change promoted to stable")
        
        print("\n" + "=" * 80)
        print("✓ CANARY DEPLOYMENT COMPLETED SUCCESSFULLY")
        print("=" * 80)
        print("\nKey Features Demonstrated:")
        print("  - Progressive traffic shifting (10% → 50% → 100%)")
        print("  - Real-time metrics comparison")
        print("  - Automated rollback on anomalies")
        print("  - Zero-downtime deployment")
        print("=" * 80)


def main():
    canary = CanaryDeployment()
    canary.run_canary_deployment()


if __name__ == "__main__":
    main()
