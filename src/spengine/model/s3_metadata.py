from pydantic import BaseModel


class S3Metadata(BaseModel):
    filename: str
    encoding: str
    data: str
