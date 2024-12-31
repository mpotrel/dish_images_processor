from datetime import datetime
from typing import Optional

from pydantic import BaseModel, Field, root_validator


class InputImageMessage(BaseModel):
    """Message format for single image processing"""
    job_id: str
    image_url: str
    created_at: str = Field(default_factory=datetime.utcnow().isoformat)
    unprocessed_image_url: Optional[str] = None

    @root_validator(pre=True)
    def duplicate_url(cls, values):
        if values.get("unprocessed_image_url") is None:
            values["unprocessed_image_url"] = values["image_url"]
        return values

class OutputImageMessage(InputImageMessage):
    processed_image_url: str
