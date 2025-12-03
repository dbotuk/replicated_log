import asyncio
import logging
import os
import socket
import sys
import time
from typing import List

from fastapi import FastAPI, HTTPException
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

        @self.app.get("/health", response_model=BaseResponse[None])
        async def get_health():
            return BaseResponse[None](
                status_code=200,
                message="Health check successful"
            )

        @self.app.post("/replicate", response_model=BaseResponse[None])
        async def replicate_message(request: BaseRequest[Message]):
            try:
                message = request.data
                logger.info(f"Replicating message {message.sequence_id} '{message.text}'")

                logger.info(f"Processing replication with {self.delay:.2f}s delay...")
                await asyncio.sleep(self.delay)

                message_added = self.messages.append(message)

                if not message_added:
                    logger.info(f"Message {message.sequence_id} '{message.text}' was not added (duplicate)")
                else:
                    logger.info(f"Message {message.sequence_id} '{message.text}' replicated successfully.")

                return BaseResponse[None](
                    status_code=200,
                    message="Message replicated successfully"
                )
            except ValueError as e:
                logger.error(f"Invalid message data: {e}")
                raise HTTPException(status_code=400, detail=f"Invalid message data: {str(e)}")
            except asyncio.CancelledError:
                logger.warning("Replication cancelled")
                raise HTTPException(status_code=408, detail="Replication request cancelled")
            except Exception as e:
                logger.error(f"Unexpected error during replication: {e}")
                raise HTTPException(status_code=500, detail="Internal server error during replication")
            
        @self.app.get("/messages", response_model=BaseResponse[list[str]])
        async def list_messages():
            try:
                messages = list(map(lambda x: x.text, self.messages.get_messages()))
                logger.info(f"Retrieved {len(messages)} replicated messages: {messages}")
                
                return BaseResponse[list[str]](
                    status_code=200,
                    message="Messages fetched successfully", 
                    data=messages
                )
            except Exception as e:
                logger.error(f"Error retrieving replicated messages: {e}")
                raise HTTPException(status_code=500, detail="Failed to retrieve replicated messages")
    
    def run(self, debug=False):
        uvicorn.run(self.app, host=self.host, port=self.port)


if __name__ == "__main__":
    host = socket.gethostname()
    port = int(os.getenv('PORT', 8051))    

    server_id = sys.argv[1] if len(sys.argv) > 1 else "follower"
    delay = float(sys.argv[2] if len(sys.argv) > 2 else "5.0")

    follower = FollowerServer(
        delay,
        host=host,
        port=port,
        server_id=server_id
    )

    follower.run(False)