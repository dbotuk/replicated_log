import atexit
import logging
import random
import threading
import time
import asyncio
import socket
import os
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, List


from fastapi import FastAPI, HTTPException
import requests
from requests.exceptions import RequestException, Timeout, ConnectionError
import uvicorn

from base import BaseRequest, BaseResponse, Message, MessageLog, MasterRequest, HealthStatus, Node, HealthChecker


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class MasterServer:
    STATUS_MULTIPLIER = {
        HealthStatus.HEALTHY: 1.0,
        HealthStatus.SUSPECTED: 3.0,
        HealthStatus.UNHEALTHY: 8.0,
    }

    def __init__(self, host='0.0.0.0', port=5000, secondaries=None, replication_timeout=20, heartbeat_interval=5.0):
        self.host = host
        self.port = port
        self.secondaries = [Node(id, url) for id, url in enumerate(secondaries)]
        self.replication_timeout = replication_timeout
        self.messages = MessageLog()
        self.message_counter = 0
        self.app = FastAPI(
            title="Master FastAPI Server",
            description="A Master FastAPI server for storing messages",
            version="1.0.0"
        )
        self.executor = ThreadPoolExecutor()
        atexit.register(self.executor.shutdown, wait=True)
        
        self.health_checker = HealthChecker(self.secondaries, logger, heartbeat_interval)
        atexit.register(self.health_checker.stop)
        self.health_checker.start()
        
        self.setup_routes()
        
        logger.info(f"Master server initialized on {host}:{port}")
        logger.info(f"Registered secondaries: {self.secondaries}")

    def setup_routes(self):

        @self.app.post("/add", response_model=BaseResponse[None])
        async def add_message(request: MasterRequest[str]):
            if not self.health_checker.quorum_status:
                raise HTTPException(status_code=500, detail="Quorum not satisfied")
            try:
                with threading.Lock():
                    self.message_counter += 1
                
                current_counter = self.message_counter

                message = Message(text=request.data)
                message.sequence_id = current_counter

                write_concern = request.write_concern

                logger.info(f"Received message {message.sequence_id} '{message.text}' with write_concern={write_concern}")
                
                message_added = self.messages.append(message)

                if not message_added:
                    logger.info(f"Message {message.sequence_id} '{message.text}' was not added.")
                else:
                    logger.info(f"Message {message.sequence_id} '{message.text}' added successfully.")

                logger.info(f"Replication started...")
                replication_success = await self._replicate(message, write_concern)
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
        
        @self.app.get("/health", response_model=BaseResponse[List[Dict]])
        async def get_health():
            try:
                health_status = [secondary.get_status() for secondary in self.secondaries]
                logger.info(f"Health check requested: {health_status}")
                
                return BaseResponse[List[Dict]](
                    status_code=200,
                    message="Health status retrieved successfully",
                    data=health_status
                )
            except Exception as e:
                logger.error(f"Error retrieving health status: {e}")
                raise HTTPException(status_code=500, detail="Failed to retrieve health status")
    
    async def _replicate(self, message: Message, write_concern: int) -> bool:
        logger.info(f"Starting replication with write_concern={write_concern}")

        time_start = time.time()
        replicated = 1

        loop = asyncio.get_running_loop()

        tasks = [
            loop.run_in_executor(self.executor, self._replicate_to_follower, secondary, message)
            for secondary in self.secondaries
        ]
            
        try:
            for completed in asyncio.as_completed(tasks, timeout=self.replication_timeout * 10):
                if replicated >= write_concern:
                    time_end = time.time()
                    logger.info(f"Write concern satisfied: {replicated}/{write_concern}")
                    logger.info(f"Replication took {round(time_end - time_start, 2)} seconds")
                    return True
                try:
                    ok = await completed
                    if ok:
                        replicated += 1
                        logger.info(f"Successful replications: {replicated}/{write_concern}")
                    else:
                        logger.warning(f"Replication failed.")

                except Exception as exc:
                    logger.error(f"Replication task raised an exception: {exc}")
        except asyncio.TimeoutError:
            logger.error("Replication timed out after 60 seconds")
            return False
        logger.warning(f"Write concern not satisfied: {replicated}/{write_concern}")
        return False
    
    def _replicate_to_follower(self, secondary: Node, message: Message) -> bool:
        attempt = 0
        
        while True:
            if attempt > 0:
                self._delay(attempt, secondary.status)
                logger.info(f"Retry replication of message {message.sequence_id} '{message.text}' to secondary {secondary.id + 1}: {secondary.url} (attempt {attempt})")
                
            if secondary.status == HealthStatus.UNHEALTHY:
                logger.warning(f"Skipping replication of message {message.sequence_id} '{message.text}'  to secondary {secondary.id + 1} ({secondary.url}): Status is {secondary.status.value}")
                attempt += 1
                continue
            
            success = self._send_message(secondary, message)
            if success:
                return True
            else:
                attempt += 1

    def _send_message(self, secondary: Node, message: Message) -> bool:
        try:
            logger.info(f"Replicating message {message.sequence_id} '{message.text}' to secondary {secondary.id + 1}: {secondary.url}")

            response = requests.post(
                f"{secondary.url}/replicate",
                json=BaseRequest(data=message).model_dump(),
                timeout=self.replication_timeout
            )

            if response.status_code == 200:
                logger.info(f"Message {message.sequence_id} '{message.text}' successfully replicated to secondary {secondary.id + 1}.")
                secondary.record_success()
                return True
            else:
                logger.warning(f"Replication of message {message.sequence_id} '{message.text}' failed to secondary {secondary.id + 1}: HTTP {response.status_code}")
                secondary.record_failure()
                return False

        except Timeout as exc:
            logger.error(f"Timeout replicating message {message.sequence_id} '{message.text}' to secondary {secondary.id + 1} ({secondary.url}): {exc}")
            secondary.record_failure()
            return False
        except ConnectionError as exc:
            logger.error(f"Connection error replicating message {message.sequence_id} '{message.text}' to secondary {secondary.id + 1} ({secondary.url}): {exc}")
            secondary.record_failure()
            return False
        except RequestException as exc:
            logger.error(f"Request error replicating message {message.sequence_id} '{message.text}' to secondary {secondary.id + 1} ({secondary.url}): {exc}")
            secondary.record_failure()
            return False
        except Exception as exc:
            logger.error(f"Unexpected error replicating message {message.sequence_id} '{message.text}' to secondary {secondary.id + 1} ({secondary.url}): {exc}")
            secondary.record_failure()
            return False
    
    def _calculate_retry_delay(self, attempt: int, status: HealthStatus, base: float = 0.5, cap: float = 60.0) -> float:
        exp = min(cap, base * (2 ** (attempt - 1)))
        adjusted = exp * self.STATUS_MULTIPLIER[status]
        return random.uniform(0, adjusted)
    
    def _delay(self, attempt: int, status: HealthStatus, base: float = 0.5, cap: float = 60.0) -> float:
        delay = self._calculate_retry_delay(attempt, status)
        logger.info(f"Waiting {delay:.2f} seconds before retrying...")
        time.sleep(delay)

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
