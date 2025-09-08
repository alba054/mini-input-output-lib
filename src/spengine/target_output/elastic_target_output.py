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
                    _index = md["_index"]
                    _id = md["_id"]
                    md = {k: v for k, v in md.items() if k not in self.metadata.exclude}

                    self.db.index(_index, md, _id)
        elif isinstance(mdata, dict):
            _index = mdata["_index"]
            _id = mdata["_id"]
            mdata = {k: v for k, v in mdata.items() if k not in self.metadata.exclude}
            self.db.index(_index, mdata, _id)
        elif mdata is None:
            return
        else:
            raise ValueError("invalid data, must be list[dict] or dict")

    def _upsert(self, mdata: list[dict] | dict, metadata: ElasticMetadata):
        if isinstance(mdata, list):
            for md in mdata:
                _index = md["_index"]
                _id = md["_id"]
                md = {k: v for k, v in md.items() if k not in self.metadata.exclude}
                self.db.upsert_data(_index, md, _id)
        elif isinstance(mdata, dict):
            _index = mdata["_index"]
            _id = mdata["_id"]
            mdata = {k: v for k, v in mdata.items() if k not in self.metadata.exclude}
            self.db.upsert_data(_index, mdata, _id)
        elif mdata is None:
            return
        else:
            raise ValueError("invalid data, must be list[dict] or dict")

    def _update_with_painless(self, mdata: list[dict] | dict, metadata: ElasticMetadata):
        if isinstance(mdata, list):
            for md in mdata:
                _index = md["_index"]
                _id = md["_id"]
                md = {k: v for k, v in md.items() if k not in self.metadata.exclude}
                self.db.upsert_data_with_painless(_index, _id, metadata.painless, md)
        elif isinstance(mdata, dict):
            _index = mdata["_index"]
            _id = mdata["_id"]
            mdata = {k: v for k, v in mdata.items() if k not in self.metadata.exclude}
            self.db.upsert_data_with_painless(_index, _id, metadata.painless, mdata)
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
            if "_index" not in data.keys() or "_id" not in data.keys():
                raise ValueError("data must have _index and _id")

        if self.metadata.mode == "index":
            self._index(data, self.metadata)
        elif self.metadata.mode == "upsert":
            self._upsert(data, self.metadata)
        elif self.metadata.mode == "update_with_painless":
            self._update_with_painless(data, self.metadata)
