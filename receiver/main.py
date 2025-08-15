import os
import redis
import json
from fastapi import FastAPI, Request, status, Response, HTTPException
from typing import Any, Dict

app = FastAPI()

redis_host = os.environ.get('REDIS_HOST', 'localhost')
r = redis.Redis(host=redis_host, port=6379, db=0, decode_responses=True)

@app.get("/health")
async def health():
    """Health check endpoint"""
    try:
        # Test Redis connection
        r.ping()
        return {"status": "healthy", "redis": "connected"}
    except Exception as e:
        return {"status": "unhealthy", "redis": f"error: {str(e)}"}

@app.post("/webhook", status_code=status.HTTP_202_ACCEPTED)
async def webhook(request: Request):
    """
    Receives a webhook and pushes the raw body to Redis.
    This avoids unnecessary JSON parsing/serialization.
    """
    try:
        body = await request.body()
        
        # Quick validation that it's valid JSON (optional)
        # This doesn't deserialize, just validates
        try:
            json.loads(body)
        except json.JSONDecodeError:
            raise HTTPException(status_code=400, detail="Invalid JSON")
        
        print(f"Received webhook: {len(body)} bytes")

        r.lpush('webhook_queue', body.decode('utf-8'))
        print("Pushed raw event to Redis queue.")
    except redis.exceptions.ConnectionError as e:
        print(f"Error pushing to Redis: {e}")
        raise HTTPException(status_code=503, detail="Could not connect to queueing service")
    except HTTPException:
        raise  # Re-raise HTTP exceptions
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error")

    # The 202 Accepted status code is automatically returned on success
    return {"message": "Accepted"}
