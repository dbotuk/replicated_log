import threading
from typing import Generic, Optional, T, Dict, List

from pydantic import BaseModel, PrivateAttr
from sortedcontainers import SortedDict
from enum import Enum
import time
import requests
import logging


class HealthStatus(str, Enum):
    HEALTHY = "Healthy"
    SUSPECTED = "Suspected"
    UNHEALTHY = "Unhealthy"

class Message(BaseModel):
    text: str
    sequence_id: int = None


class MessageLog(BaseModel):
    model_config = {"arbitrary_types_allowed": True}
    
    messages: SortedDict[int, Message] = SortedDict()
    _lock: threading.Lock = PrivateAttr(default_factory=threading.Lock)

    def append(self, item):
        with self._lock:
            if item.sequence_id not in self.messages.keys():
                self.messages[item.sequence_id] = item
                return True
            else:
                return False
    
    def get_messages(self):
        with self._lock:
            return list(self.messages.values())


class BaseRequest(BaseModel, Generic[T]):
    data: T


class BaseResponse(BaseModel, Generic[T]):
    status_code: Optional[int] = None
    message: Optional[str] = None
    data: Optional[T] = None


class MasterRequest(BaseRequest):
    write_concern: int


class Node:
    def __init__(self, id: int, url: str):
        self.id = id
        self.url = url
        self.status = HealthStatus.HEALTHY
        self.last_heartbeat = time.time()
        self.consecutive_failures = 0
        self.lock = threading.Lock()
    
    def record_success(self):
        with self.lock:
            self.last_heartbeat = time.time()
            if self.status == HealthStatus.SUSPECTED:
                self.status = HealthStatus.HEALTHY
                self.consecutive_failures = 0
            elif self.status == HealthStatus.UNHEALTHY:
                self.status = HealthStatus.SUSPECTED
                self.consecutive_failures = 0
    
    def record_failure(self):
        with self.lock:
            self.consecutive_failures += 1
            if self.status == HealthStatus.HEALTHY:
                self.status = HealthStatus.SUSPECTED
            elif self.status == HealthStatus.SUSPECTED and self.consecutive_failures >= 3:
                self.status = HealthStatus.UNHEALTHY
    
    def get_status(self) -> Dict:
        with self.lock:
            return {
                "id": self.id,
                "url": self.url,
                "status": self.status.value,
                "last_heartbeat": self.last_heartbeat,
                "consecutive_failures": self.consecutive_failures
            }

class HealthChecker:
    def __init__(self, nodes: List[Node], logger: logging.Logger, heartbeat_interval: float = 5.0):
        self.nodes = nodes
        self.logger = logger
        self.heartbeat_interval = heartbeat_interval
        self.heartbeat_thread = None
        self.heartbeat_running = False
        self.quorum_number = (len(self.nodes) + 1) // 2 + 1
        self.quorum_status = False
    
    def _send_heartbeat(self, node: Node) -> bool:
        try:
            response = requests.get(
                f"{node.url}/health",
                timeout=self.heartbeat_interval
            )
            return response.status_code == 200
        except Exception as exc:
            self.logger.debug(f"Heartbeat failed for {node.url}: {exc}")
            return False
    
    def _heartbeat_worker(self):
        self.logger.info("Heartbeat worker started")
        while self.heartbeat_running:
            try:
                healthy_nodes = 0
                for node in self.nodes:
                    previous_status = node.status

                    if not self.heartbeat_running:
                        break
                    
                    success = self._send_heartbeat(node)
                    
                    if success:
                        healthy_nodes += 1
                        node.record_success()
                        if previous_status == node.status:
                            continue
                        self.logger.info(f"Heartbeat success for {node.url}: {node.status.value}")
                    else:
                        node.record_failure()
                        if previous_status == node.status:
                            continue
                        self.logger.warning(
                            f"Heartbeat failure for {node.url}: "
                            f"Status -> {node.status.value} "
                            f"(failures: {node.consecutive_failures})"
                        )
                
                with threading.Lock():
                    previous_quorum_status = self.quorum_status
                    self.quorum_status = (healthy_nodes + 1) >= self.quorum_number
                    if previous_quorum_status != self.quorum_status:
                        if self.quorum_status:
                            self.logger.info(f"Quorum is satisfied!")
                        else:
                            self.logger.warning(f"Quorum is not satisfied!")

                time.sleep(self.heartbeat_interval)
            except Exception as exc:
                self.logger.error(f"Error in heartbeat worker: {exc}")
                time.sleep(self.heartbeat_interval)
        
        self.logger.info("Heartbeat worker stopped")
    
    def start(self):
        if not self.heartbeat_running and self.nodes:
            self.heartbeat_running = True
            self.heartbeat_thread = threading.Thread(
                target=self._heartbeat_worker,
                daemon=True,
                name="HeartbeatThread"
            )
            self.heartbeat_thread.start()
            self.logger.info(f"Heartbeat mechanism started with interval {self.heartbeat_interval}s")
    
    def stop(self):
        if self.heartbeat_running:
            self.heartbeat_running = False
            if self.heartbeat_thread:
                self.heartbeat_thread.join(timeout=5.0)
            self.logger.info("Heartbeat mechanism stopped")