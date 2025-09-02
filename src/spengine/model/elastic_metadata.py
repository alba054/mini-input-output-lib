from dataclasses import dataclass


@dataclass
class ElasticMetadata:
    exclude: list[str]
    mode: str = "index"  # upsert | index | update_with_painless
    painless: str = None  # only if mode 'update_with_painless'
    bulk: bool = True  # if data is a list of dictionaries then true means bulk insert the data
