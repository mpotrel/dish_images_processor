from datetime import datetime

from pydantic import BaseModel


class InputImageMessage(BaseModel):
    """Message format for single image processing"""
    job_id: str
    image_url: str
    submitted_at: datetime

class OutputImageMessage(InputImageMessage):
    processed_url: str
