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

    # for iterating dictionary and replacing the value by 'replace' arg, including handling nested lists
    def _recursively_iterate_dict_and_replace(self, d: dict, replace: dict):
        for key in d.keys():
            if isinstance(d[key], dict):
                self._recursively_iterate_dict_and_replace(d[key], replace)
            elif isinstance(d[key], list):
                d[key] = self._recursively_iterate_list_and_replace(d[key], replace)
            else:
                if d[key] in replace.keys():
                    d[key] = replace[d[key]]

    # helper function to handle nested lists
    def _recursively_iterate_list_and_replace(self, lst: list, replace: dict):
        for i, item in enumerate(lst):
            if isinstance(item, dict):
                self._recursively_iterate_dict_and_replace(item, replace)
            elif isinstance(item, list):
                lst[i] = self._recursively_iterate_list_and_replace(item, replace)
            else:
                if item in replace.keys():
                    lst[i] = replace[item]
        return lst

    # {"id": "<id>"}
    def _update_by_query(self, mdata: list[dict] | dict, metadata: ElasticMetadata):
        if isinstance(mdata, list):
            for md in mdata:
                _index = md["_index"]
                query_params = {f"<{k}>": md[k] for k in metadata.query_params}
                md = {k: v for k, v in md.items() if k not in self.metadata.exclude}
                md = {k: v for k, v in md.items() if k not in self.metadata.query_params}

                self._recursively_iterate_dict_and_replace(metadata.query, query_params)

                self.db.update_by_query(
                    index=_index, query=metadata.query, painless_script=metadata.painless, params=md
                )
        elif isinstance(mdata, dict):
            _index = mdata["_index"]
            query_params = {f"<{k}>": mdata[k] for k in metadata.query_params}
            mdata = {k: v for k, v in mdata.items() if k not in self.metadata.exclude}
            mdata = {k: v for k, v in mdata.items() if k not in self.metadata.query_params}

            self._recursively_iterate_dict_and_replace(metadata.query, query_params)

            self.db.update_by_query(index=_index, query=metadata.query, painless_script=metadata.painless, params=mdata)
        elif mdata is None:
            return
        else:
            raise ValueError("invalid data, must be list[dict] or dict")

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
        elif self.metadata.mode == "update_by_query":
            self._update_by_query(data, self.metadata)
