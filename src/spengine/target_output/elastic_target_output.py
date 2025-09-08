from spengine.base.observer import BaseObserver
from spengine.data_source.data_source_subject import DataSourceSubject
from spengine.model.elastic_metadata import ElasticMetadata
from spengine.app._elastic import ElasticClient


class EsTargetOutput(BaseObserver):
    metadatas: ElasticMetadata

    # the order of processor matters
    # because processor will be called in the order of the list
    def __init__(
        self,
        db: ElasticClient,
        metadata: ElasticMetadata,
    ):
        super().__init__()
        self.db = db
        self.metadata = metadata

    def _index(self, mdata: list[dict] | dict, metadata: ElasticMetadata):
        if isinstance(mdata, list):
            if metadata.bulk:
                self.db.bulk_insert(mdata)
            else:
                for md in mdata:
                    self.db.index(md["_index"], md, md["_id"])
        elif isinstance(mdata, dict):
            self.db.index(mdata["_index"], mdata, mdata["_id"])
        elif mdata is None:
            return
        else:
            raise ValueError("invalid data, must be list[dict] or dict")

    def _upsert(self, mdata: list[dict] | dict, metadata: ElasticMetadata):
        if isinstance(mdata, list):
            for md in mdata:
                self.db.upsert_data(md["_index"], md, md["_id"])
        elif isinstance(mdata, dict):
            self.db.index(mdata["_index"], mdata, mdata["_id"])
        elif mdata is None:
            return
        else:
            raise ValueError("invalid data, must be list[dict] or dict")

    def _update_with_painless(self, mdata: list[dict] | dict, metadata: ElasticMetadata):
        if isinstance(mdata, list):
            for md in mdata:
                self.db.upsert_data_with_painless(md["_index"], md["_id"], metadata.painless, md)
        elif isinstance(mdata, dict):
            self.db.upsert_data_with_painless(mdata["_index"], mdata["_id"], metadata.painless, mdata)
        elif mdata is None:
            return
        else:
            raise ValueError("invalid data, must be list[dict] or dict")

    def _validate_list_of_dictionaries(self, data: list[dict]) -> list[dict]:
        return [d for d in data if "_index" in d.keys() and "_id" in d.keys()]

    def update(self, subject: DataSourceSubject):
        data = subject.data

        if isinstance(data, list):
            data = self._validate_list_of_dictionaries(data)
        elif isinstance(data, dict):
            data = {k: v for k, v in data.items() if k not in self.metadata.exclude}
            if "_index" not in data.keys() or "_id" not in data.keys():
                raise ValueError("data must have _index and _id")

        if self.metadata.mode == "index":
            self._index(data, self.metadata)
        elif self.metadata.mode == "upsert":
            self._upsert(data, self.metadata)
        elif self.metadata.mode == "update_with_painless":
            self._update_with_painless(data, self.metadata)
