import logging
import sys
import threading
import time
import queue
from typing import Dict, List

from fastapi import FastAPI
import requests
import uvicorn

from base import BaseRequest, BaseResponse, Message, MessageLog, MasterRequest


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
        self.messages = MessageLog()
        self.app = FastAPI(
            title="Master FastAPI Server",
            description="A Master FastAPI server for storing messages",
            version="1.0.0"
        )
        self.setup_routes()
        self.message_counter = 0
        
        logger.info(f"Master server initialized on {host}:{port}")
        logger.info(f"Registered secondaries: {self.secondaries}")

    def setup_routes(self):
        """Setup HTTP routes for the master server."""

        @self.app.post("/add", response_model=BaseResponse[None])
        async def add_message(request: MasterRequest[str]):
            self.message_counter += 1

            message = Message(text=request.data)
            message.sequence_id = self.message_counter
            write_concern = request.write_concern

            logger.info(f"Received message: {message} with write_concern={write_concern}")
            
            message_added = self.messages.append(message)

            if not message_added:
                logger.info(f"Message {message} was not added (duplicate)")
            else:
                logger.info(f"Message added successfully: {message}")

            logger.info(f"Replication started...")
            replication_success = self.replicate(message, write_concern)
            logger.info(f"Replication finished...")

            if replication_success:
                return BaseResponse[None](
                    status_code=200,
                    message=f"Message added successfully with write_concern={write_concern}"
                )
            else:
                return BaseResponse[None](
                    status_code=500,
                    message=f"Message added to master but write_concern={write_concern} not satisfied"
                )

        @self.app.get("/messages", response_model=BaseResponse[list[str]])
        async def list_messages():
            messages = list(map(lambda x: x.text, self.messages.get_messages()))
            logger.info(f"Retrieved {len(messages)} messages from master: {messages}")
            
            return BaseResponse[list[str]](
                status_code=200,
                message="Messages fetched successfully", 
                data=messages
            )
    
    def replicate(self, message: Message, write_concern: int):
        max_concern = len(self.secondaries) + 1
        write_concern = min(write_concern, max_concern)
        
        logger.info(f"Starting replication with write_concern={write_concern}")
        
        threads = []
        result_queue = queue.Queue()
        
        def replicate_to_follower(secondary_url: str, message: Message, index: int):
            try:
                logger.info(f"Replicating message to secondary {index + 1}: {secondary_url}")
                response = requests.post(
                    f"{secondary_url}/replicate",
                    json=BaseRequest(data=message).model_dump(),
                    timeout=60
                )

                if response.status_code == 200:
                    logger.info(f"Message successfully replicated to secondary {index + 1}.")
                    result_queue.put(True)
                else:
                    logger.info(f"Failure during replication of message to secondary {index + 1}.")
                    result_queue.put(False)
            except Exception as e:
                logger.error(f"Error replicating to secondary {index + 1}: {e}")
                result_queue.put(False)
        
        time_start = time.time()

        for i, secondary_url in enumerate(self.secondaries):
            thread = threading.Thread(
                target=replicate_to_follower,
                args=(secondary_url, message, i)
            )
            threads.append(thread)
            thread.start()
        
        successful_replications = 1
        required_replications = write_concern - 1
        
        logger.info(f"Waiting for {required_replications} successful replications...")
        
        while successful_replications < write_concern:
            try:
                result = result_queue.get(timeout=1)
                if result:
                    successful_replications += 1
                    logger.info(f"Successful replications: {successful_replications}/{write_concern}")
                    
                    if successful_replications >= write_concern:
                        logger.info(f"Write concern satisfied: {successful_replications}/{write_concern}")
                        break
            except queue.Empty:
                continue
        
        time_end = time.time()
        logger.info(f"Replication took {round(time_end - time_start, 2)} seconds")

        if successful_replications >= write_concern:
            logger.info(f"Write concern satisfied: {successful_replications}/{write_concern} (including master)")
            return True
        else:
            logger.error(f"Write concern not satisfied: {successful_replications}/{write_concern} (including master)")
            return False
    
    def run(self, debug=False):
        """Start the master server."""
        uvicorn.run(self.app, host=self.host, port=self.port)


if __name__ == "__main__":
    host = sys.argv[1] if len(sys.argv) > 1 else "0.0.0.0"
    port = int(sys.argv[2]) if len(sys.argv) > 2 else 8051
    secondaries = sys.argv[3:] if len(sys.argv) > 3 else []

    master = MasterServer(
        host=host,
        port=port,
        secondaries=secondaries
    )

    master.run()