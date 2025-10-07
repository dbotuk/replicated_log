from typing import Generic, Optional, T

from pydantic import BaseModel


class Message(BaseModel):
    text: str


class BaseRequest(BaseModel, Generic[T]):
    data: T


class BaseResponse(BaseModel, Generic[T]):
    status_code: Optional[int] = None
    message: Optional[str] = None
    data: Optional[T] = None