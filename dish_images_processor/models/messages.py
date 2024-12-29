from datetime import datetime

from pydantic import BaseModel, Field


class InputImageMessage(BaseModel):
    """Message format for single image processing"""
    job_id: str
    image_url: str
    created_at: datetime = Field(default_factory=datetime.utcnow)

class OutputImageMessage(InputImageMessage):
    processed_url: str
