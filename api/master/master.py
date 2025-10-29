import logging
import threading
import time
import asyncio
import socket
import os
from concurrent.futures import ThreadPoolExecutor
from typing import List, Set 

from fastapi import FastAPI, HTTPException
import requests
from requests.exceptions import RequestException, Timeout, ConnectionError
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
        self.message_counter = 0
        self.app = FastAPI(
            title="Master FastAPI Server",
            description="A Master FastAPI server for storing messages",
            version="1.0.0"
        )
        self.setup_routes()
        
        logger.info(f"Master server initialized on {host}:{port}")
        logger.info(f"Registered secondaries: {self.secondaries}")

    def setup_routes(self):

        @self.app.post("/add", response_model=BaseResponse[None])
        async def add_message(request: MasterRequest[str]):
            try:
                with threading.Lock():
                    self.message_counter += 1
                
                current_counter = self.message_counter

                message = Message(text=request.data)
                message.sequence_id = current_counter

                write_concern = request.write_concern

                logger.info(f"Received message: {message} with write_concern={write_concern}")
                
                message_added = self.messages.append(message)

                if not message_added:
                    logger.info(f"Message {message} was not added.")
                else:
                    logger.info(f"Message added successfully: {message}")

                logger.info(f"Replication started...")
                replication_success = await self.replicate(message, write_concern)
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
            except ValueError as e:
                logger.error(f"Invalid request data: {e}")
                raise HTTPException(status_code=400, detail=f"Invalid request data: {str(e)}")
            except Exception as e:
                logger.error(f"Unexpected error in add_message: {e}")
                raise HTTPException(status_code=500, detail="Internal server error occurred while processing message")

        @self.app.get("/messages", response_model=BaseResponse[list[str]])
        async def list_messages():
            try:
                messages = list(map(lambda x: x.text, self.messages.get_messages()))
                logger.info(f"Retrieved {len(messages)} messages from master: {messages}")
                
                return BaseResponse[list[str]](
                    status_code=200,
                    message="Messages fetched successfully", 
                    data=messages
                )
            except Exception as e:
                logger.error(f"Error retrieving messages: {e}")
                raise HTTPException(status_code=500, detail="Failed to retrieve messages")
    
    async def replicate(self, message: Message, write_concern: int) -> bool:
        logger.info(f"Starting replication with write_concern={write_concern}")

        time_start = time.time()
        replicated = 1

        loop = asyncio.get_running_loop()
        executor = ThreadPoolExecutor(max_workers=max(1, len(self.secondaries)))

        tasks = [
                    loop.run_in_executor(executor, self._replicate_to_follower, secondary_url, message, index)
                    for index, secondary_url 
                    in enumerate(self.secondaries)
                ]
    
        try:
            for index, completed in enumerate(asyncio.as_completed(tasks, timeout=60)):
                try:
                    ok = await completed
                except Exception as exc:
                    logger.error(f"Replication task raised for secondary {index + 1} ({self.secondaries[index]}): {exc}")

                if ok:
                    replicated += 1
                    logger.info(f"Successful replications: {replicated}/{write_concern}")

                    if replicated >= write_concern:
                        time_end = time.time()
                        logger.info(f"Write concern satisfied: {replicated}/{write_concern}")
                        logger.info(f"Replication took {round(time_end - time_start, 2)} seconds")
                        return True
                else:
                    logger.warning(f"Replication failed for secondary {index + 1} ({self.secondaries[index]}).")

        except asyncio.TimeoutError:
            logger.error("Replication timed out after 60 seconds")
            return False

        logger.warning(f"Write concern not satisfied: {replicated}/{write_concern}")
        return False

    def _replicate_to_follower(self, secondary_url: str, message: Message, index: int) -> bool:
        try:
            logger.info(f"Replicating message to secondary {index + 1}: {secondary_url}")

            response = requests.post(
                f"{secondary_url}/replicate",
                json=BaseRequest(data=message).model_dump(),
                timeout=60
            )

            if response.status_code == 200:
                logger.info(f"Message successfully replicated to secondary {index + 1}.")
                return True

            logger.warning(
                f"Replication failed to secondary {index + 1}: HTTP {response.status_code}"
            )
            return False

        except Timeout as exc:
            logger.error(
                f"Timeout replicating to secondary {index + 1} ({secondary_url}): {exc}"
            )
            return False
        except ConnectionError as exc:
            logger.error(
                f"Connection error replicating to secondary {index + 1} ({secondary_url}): {exc}"
            )
            return False
        except RequestException as exc:
            logger.error(
                f"Request error replicating to secondary {index + 1} ({secondary_url}): {exc}"
            )
            return False
        except Exception as exc:
            logger.error(
                f"Unexpected error replicating to secondary {index + 1} ({secondary_url}): {exc}"
            )
            return False
    
    def run(self, debug=False):
        uvicorn.run(self.app, host=self.host, port=self.port)


if __name__ == "__main__":
    host = socket.gethostname()
    port = int(os.getenv('PORT', 8051))   
    secondaries = [url.strip() for url in os.getenv('SECONDARIES', '').split(',')] if os.getenv('SECONDARIES') else []

    master = MasterServer(
        host=host,
        port=port,
        secondaries=secondaries
    )

    master.run()
