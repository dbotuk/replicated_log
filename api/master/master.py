import logging
import sys
import threading
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


class MasterServer:
    def __init__(self, host='0.0.0.0', port=5000, secondaries=None):
        self.host = host
        self.port = port
        self.secondaries = secondaries or []
        self.messages = []
        self.app = FastAPI(
            title="Master FastAPI Server",
            description="A Master FastAPI server for storing messages",
            version="1.0.0"
        )
        self.setup_routes()
        
        logger.info(f"Master server initialized on {host}:{port}")
        logger.info(f"Registered secondaries: {self.secondaries}")

    def setup_routes(self):
        """Setup HTTP routes for the master server."""

        @self.app.post("/add", response_model=BaseResponse[None])
        async def add_message(request: BaseRequest[Message]):
            message = request.data

            logger.info(f"Received message: {message}")
            self.messages.append(message)

            logger.info(f"Replication started...")
            self.replicate(message)
            logger.info(f"Replication finished...")

            return BaseResponse[None](
                status_code=200,
                message="Message added successfully"
            )


        @self.app.get("/messages", response_model=BaseResponse[List[Message]])
        async def list_messages():
            logger.info(f"Retrieved {len(self.messages)} replicated messages.")
            return BaseResponse[List[Message]](
                status_code=200,
                message="Messages fetched successfully", 
                data=self.messages
            )
    
    def replicate(self, message: Message):
        threads = []
        results = []
        
        def replicate_to_follower(secondary_url: str, message: Message, index: int):
            try:
                logger.info(f"Replicating message to secondary {index + 1}: {secondary_url}")
                response = requests.post(
                    f"{secondary_url}/add",
                    json=BaseRequest(data=message).dict(),
                    timeout=10
                )

                if response.status_code == 200:
                    logger.info(f"Message successfully replicated to secondary {index + 1}.")
                    results.append(True)
                else:
                    logger.info(f"Failure during replication of message to secondary {index + 1}.")
                    results.append(False)
            except Exception as e:
                logger.error(f"Error replicating to secondary {index + 1}: {e}")
                results.append(False)
        
        time_start = time.time()
        # Start replication threads
        for i, secondary_url in enumerate(self.secondaries):
            thread = threading.Thread(
                target=replicate_to_follower,
                args=(secondary_url, message, i)
            )
            threads.append(thread)
            thread.start()
            
        # Wait for all threads to complete
        for thread in threads:
            thread.join()
        
        time_end = time.time()
        logger.info(f"Replication took {round(time_end - time_start, 2)} seconds")

        # Check if all replications were successful
        success_count = sum(results)
        total_followers = len(self.secondaries)
            
        if success_count == total_followers:
            logger.info(f"Successfully replicated to all {total_followers} secondaries")
            return True
        else:
            logger.error(f"Replicated only to {success_count}/{total_followers} secondaries")
            return False
    
    def run(self, debug=False):
        """Start the master server."""
        uvicorn.run(self.app, host=self.host, port=self.port)


if __name__ == "__main__":
    host = sys.argv[1] if len(sys.argv) > 1 else "0.0.0.0"
    port = int(sys.argv[2]) if len(sys.argv) > 2 else 8051
    # Remaining args are secondary URLs
    secondaries = sys.argv[3:] if len(sys.argv) > 3 else []

    master = MasterServer(
        host=host,
        port=port,
        secondaries=secondaries
    )

    master.run()