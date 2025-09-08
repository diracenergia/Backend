from pydantic import BaseModel
from typing import Literal, Optional

StatusLit = Literal["queued", "sent", "acked", "failed", "expired"]

class CommandStatusIn(BaseModel):
    status: Literal["sent", "acked", "failed", "expired"]
    error: Optional[str] = None

    class Config:
        extra = "ignore"
