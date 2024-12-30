from pydantic import BaseModel, HttpUrl, validator


class ProcessingResponse(BaseModel):
    job_id: str
    status: str

class ImageUrls(BaseModel):
    images: list[HttpUrl]

    @validator("images")
    def validate_list(cls, v):
        if not v:
            raise ValueError("Image list cannot be empty")
        return v
