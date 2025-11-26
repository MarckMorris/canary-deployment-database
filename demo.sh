#!/bin/bash
echo "Starting Canary Deployment System..."
docker-compose up -d
sleep 15
python src/canary_deployment.py
