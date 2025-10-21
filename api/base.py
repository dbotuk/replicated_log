import threading
from typing import Generic, Optional, T

from pydantic import BaseModel, PrivateAttr


class Message(BaseModel):
    text: str


class MessageLog(BaseModel):
    messages: list[Message] = []
    _lock: threading.Lock = PrivateAttr(default_factory=threading.Lock)

    def append(self, item):
        with self._lock:
            self.messages.append(item)

    def remove(self, item):
        with self._lock:
            self.messages.remove(item)

    def get(self, index):
        with self._lock:
            return self.messages[index]

    def len(self):
        with self._lock:
            return len(self.messages)


class BaseRequest(BaseModel, Generic[T]):
    data: T


class BaseResponse(BaseModel, Generic[T]):
    status_code: Optional[int] = None
    message: Optional[str] = None
    data: Optional[T] = None