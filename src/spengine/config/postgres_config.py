from pydantic.v1 import BaseSettings


class PostgreConfig(BaseSettings):
    PG_DB: str
    PG_USER: str
    PG_PASS: str
    PG_HOST: str
    PG_PORT: int

    class Config:
        env = ".env"
