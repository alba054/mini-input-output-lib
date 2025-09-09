from dataclasses import dataclass


@dataclass
class ElasticMetadata:
    exclude: list[str]
    mode: str = "index"  # upsert | index | update_with_painless | update_by_query
    painless: str = None  # only if mode 'update_with_painless' or 'update_by_query'
    bulk: bool = True  # if data is a list of dictionaries then true means bulk insert the data
    query: dict = None  # if mode is 'update_by_query'
    query_params: list[str] = None  # exclude from data and take as query variables
