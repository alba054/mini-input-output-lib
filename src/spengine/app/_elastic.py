import elasticsearch
from loguru import logger

from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk


class ElasticClient:
    es: Elasticsearch

    def __init__(self, host: str, port: int, username: str, password: str):
        self.es = Elasticsearch(
            [
                {
                    "host": host,
                    "port": port,
                    "scheme": "http",
                }
            ],
            http_auth=(
                username,
                password,
            ),
        )

    def get_single(self, index: str, size=1, query: dict = None):
        data = self.es.search(index=index, size=size, body=query)

        if len(data["hits"]["hits"]) == 0:
            return None

        return data["hits"]["hits"]

    def scroll_v2(
        self,
        index: str,
        query: dict = None,
    ):
        try:
            size = 1000
            data = self.es.search(
                index=index,
                size=size,
                scroll="30m",
                body=query,
            )
            sid = data["_scroll_id"]
            scroll_size = len(data["hits"]["hits"])
            while scroll_size > 0:
                try:
                    hits = data["hits"]["hits"]
                    for hit in hits:
                        source = hit["_source"]
                        source["_id"] = hit["_id"]
                        source["_index"] = hit["_index"]
                        yield source

                    data = self.es.scroll(scroll_id=sid, scroll="10m")
                    sid = data["_scroll_id"]
                    scroll_size = len(hits)
                except Exception as e:
                    logger.error(e)

        except KeyboardInterrupt:
            exit()

    def deep_paginate(
        self,
        index: str,
        query: dict = None,
    ):
        # query should be {"query"} there is query key
        pit_id = None
        try:
            pit_keep_alive = "30m"
            pit = self.es.open_point_in_time(index=index, keep_alive=pit_keep_alive)
            pit_id = pit["id"]
            search_after = None

            counter = 0
            while True:
                try:
                    body = {
                        "size": 1000,
                        "pit": {"id": pit_id, "keep_alive": pit_keep_alive},
                        "sort": [{"crawling_time": "asc"}],  # Ensure a consistent sort order
                    }
                    if query is not None:
                        body = {**body, **query}

                    if search_after:
                        body["search_after"] = search_after

                    response = self.es.search(body=body)
                    hits = response.get("hits", {}).get("hits", [])

                    if not hits:
                        break  # No more results

                    for hit in hits:
                        source = hit["_source"]
                        source["_id"] = hit["_id"]
                        yield source

                    logger.info(f"processed {counter}")

                    search_after = hits[-1]["sort"]
                except Exception as e:
                    logger.error(e)

            self.es.close_point_in_time(body={"id": pit_id})
        except KeyboardInterrupt:
            exit(1)
        except elasticsearch.exceptions.NotFoundError:
            return

    def upsert_data(self, index, data, id):
        body = {"doc": data, "doc_as_upsert": True}
        result = self.es.update(index=index, id=id, body=body, retry_on_conflict=3)
        logger.info(result)
        logger.success(f"Success upsert data to Index = '{index}' || '{id}'")

    def index(self, index, data, id):
        result = self.es.index(index=index, id=id, document=data)
        logger.info(result)
        logger.success(f"Success index data to = '{index}' || '{id}'")

    def bulk_insert(self, data_list):
        actions = [
            {
                "_index": data["_index"],  # Use _index from the data
                "_id": data["_id"],  # Use _id from the data
                "_source": data,
            }
            for data in data_list
        ]

        success, _ = bulk(self.es, actions)
        logger.success(f"Successfully inserted {success} documents into their respective indices")

    def upsert_data_with_painless(self, index, id, painless_script, params):
        result = self.es.update(
            index=index,
            id=id,
            body={
                "script": {
                    "source": painless_script,
                    "lang": "painless",
                    "params": params,
                },
            },
        )
        logger.info(result)
        logger.success(f"Success update data to = '{index}' || '{id}'")

    def update_by_query(self, index, query, painless_script, params):
        _ = self.es.update_by_query(
            index=index,
            body={"script": {"source": painless_script, "lang": "painless", "params": params}, "query": query},
        )

        logger.info(f"success updating {index}")
