from dataclasses import dataclass


@dataclass
class MapperContext:
    src: str
    to: str


@dataclass
class MapperValueContext:
    ctx: list[MapperContext]
    data: dict
