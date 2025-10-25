import asyncio
import logging
import random
import sys
import time
from typing import Dict, List

from fastapi import FastAPI
import requests
import uvicorn

from base import BaseRequest, BaseResponse, Message, MessageLog


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class FollowerServer:
    def __init__(self, delay, host='0.0.0.0', port=5000, server_id='follower'):
        self.delay = delay
        self.host = host
        self.port = port
        self.server_id = server_id
        self.messages = MessageLog()
        self.app = FastAPI(
            title="Secondary FastAPI Server",
            description="A Secondary FastAPI server for replicating messages",
            version="1.0.0"
        )
        self.setup_routes()
        
        logger.info(f"Secondary server {server_id} initialized on {host}:{port}")

    def setup_routes(self):
        """Setup HTTP routes for the secondary server."""

        @self.app.post("/replicate", response_model=BaseResponse[None])
        async def replicate_message(request: BaseRequest[Message]):
            message = request.data
            logger.info(f"Replicating message: {message}")

            logger.info(f"Processing replication with {self.delay:.2f}s delay...")
            await asyncio.sleep(self.delay)

            message_added = self.messages.append(message)

            if not message_added:
                logger.info(f"Message {message} was not added (duplicate)")
            else:
                logger.info(f"Message replicated successfully: {message}")

            return BaseResponse[None](
                status_code=200,
                message="Message replicated successfully"
            )
            
        @self.app.get("/messages", response_model=BaseResponse[list[str]])
        async def list_messages():
            messages = list(map(lambda x: x.text, self.messages.get_messages()))
            logger.info(f"Retrieved {len(messages)} replicated messages: {messages}")
            
            return BaseResponse[list[str]](
                status_code=200,
                message="Messages fetched successfully", 
                data=messages
            )
    
    def run(self, debug=False):
        """Start the secondary server."""
        uvicorn.run(self.app, host=self.host, port=self.port)


if __name__ == "__main__":
    host = sys.argv[1] if len(sys.argv) > 1 else "0.0.0.0"
    port = int(sys.argv[2]) if len(sys.argv) > 2 else 8052
    server_id = sys.argv[3] if len(sys.argv) > 3 else "follower"
    delay = sys.argv[4] if len(sys.argv) > 4 else "5.0"
    delay = float(delay)

    follower = FollowerServer(
        delay,
        host=host,
        port=port,
        server_id=server_id
    )

    follower.run(False)