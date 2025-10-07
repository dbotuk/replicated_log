import logging
import random
import sys
import time
from typing import Dict, List

from fastapi import FastAPI
import requests
import uvicorn

from base import BaseRequest, BaseResponse, Message


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class FollowerServer:
    def __init__(self, host='0.0.0.0', port=5000, server_id='follower'):
        self.host = host
        self.port = port
        self.server_id = server_id
        self.messages = []
        self.app = FastAPI(
            title="Secondary FastAPI Server",
            description="A Secondary FastAPI server for replicating messages",
            version="1.0.0"
        )
        self.setup_routes()
        
        logger.info(f"Secondary server {server_id} initialized on {host}:{port}")

    def setup_routes(self):
        """Setup HTTP routes for the secondary server."""

        @self.app.post("/add", response_model=BaseResponse[None])
        async def add_message(request: BaseRequest[Message]):
            logger.info(f"Replicating message: {request.data}")

            delay = random.uniform(2.0, 5.0)
            logger.info(f"Processing replication with {delay:.2f}s delay...")
            time.sleep(delay)

            self.messages.append(request.data)
            logger.info(f"Message replicated successfully: {request.data}")
            return BaseResponse[None](
                status_code=200,
                message="Message replicated successfully"
            )


        @self.app.get("/messages", response_model=BaseResponse[List[Message]])
        async def list_messages():
            logger.info(f"Retrieved {len(self.messages)} replicated messages.")
            return BaseResponse[List[Message]](
                status_code=200,
                message="Messages fetched successfully", 
                data=self.messages
            )
    
    def run(self, debug=False):
        """Start the secondary server."""
        uvicorn.run(self.app, host=self.host, port=self.port)


if __name__ == "__main__":
    host = sys.argv[1] if len(sys.argv) > 1 else "0.0.0.0"
    port = int(sys.argv[2]) if len(sys.argv) > 2 else 8052
    server_id = sys.argv[3] if len(sys.argv) > 3 else "follower"

    follower = FollowerServer(
        host=host,
        port=port,
        server_id=server_id
    )

    follower.run(False)