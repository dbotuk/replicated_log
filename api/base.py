import threading
from typing import Generic, Optional, T

from pydantic import BaseModel, PrivateAttr
from sortedcontainers import SortedDict


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