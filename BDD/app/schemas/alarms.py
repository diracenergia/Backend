from pydantic import BaseModel
from typing import Optional

class AckIn(BaseModel):
    user: str
    note: Optional[str] = None
