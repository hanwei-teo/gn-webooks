import os
import redis
import json
from fastapi import FastAPI, Request, status, Response, HTTPException
from typing import Any, Dict

app = FastAPI()

redis_host = os.environ.get('REDIS_HOST', 'localhost')
r = redis.Redis(host=redis_host, port=6379, db=0, decode_responses=True)

# Webhook configuration
WEBHOOK_QUEUE_PREFIX = os.environ.get('WEBHOOK_QUEUE_PREFIX', 'webhook')

@app.get("/health")
async def health():
    """Health check endpoint"""
    try:
        # Test Redis connection
        r.ping()
        return {"status": "healthy", "redis": "connected"}
    except Exception as e:
        return {"status": "unhealthy", "redis": f"error: {str(e)}"}

@app.post("/webhook/bugsnag", status_code=status.HTTP_202_ACCEPTED)
async def bugsnag_webhook(request: Request):
    """
    Receives a Bugsnag webhook and pushes the raw body to Redis.
    """
    return await process_webhook(request, "bugsnag")

@app.post("/webhook", status_code=status.HTTP_202_ACCEPTED)
async def generic_webhook(request: Request):
    """
    Generic webhook endpoint (legacy support).
    """
    return await process_webhook(request, "generic")

async def process_webhook(request: Request, webhook_type: str):
    """
    Generic webhook processor that routes to appropriate Redis queue.
    """
    try:
        body = await request.body()
        
        # Quick validation that it's valid JSON (optional)
        try:
            json.loads(body)
        except json.JSONDecodeError:
            raise HTTPException(status_code=400, detail="Invalid JSON")
        
        # Route to appropriate queue based on webhook type
        queue_name = f"{WEBHOOK_QUEUE_PREFIX}_{webhook_type}_queue"
        
        print(f"Received {webhook_type} webhook: {len(body)} bytes")
        r.lpush(queue_name, body.decode('utf-8'))
        print(f"Pushed raw event to Redis queue: {queue_name}")
        
    except redis.exceptions.ConnectionError as e:
        print(f"Error pushing to Redis: {e}")
        raise HTTPException(status_code=503, detail="Could not connect to queueing service")
    except HTTPException:
        raise  # Re-raise HTTP exceptions
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error")

    return {"message": "Accepted"}
