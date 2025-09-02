from spengine.app.postgre import PgSaService
from spengine.base.observer import BaseObserver
from spengine.data_source.data_source_subject import DataSourceSubject
from spengine.model.pg_metadata import PgMetadata


class PgTargetOutput(BaseObserver):
    metadatas: list[PgMetadata]

    # the order of processor matters
    # because processor will be called in the order of the list
    def __init__(
        self,
        db: PgSaService,
        metadatas: list[PgMetadata],
    ):
        super().__init__()
        self.db = db
        self.metadatas = metadatas

    def _exclude_data(self, metadata: PgMetadata, data: dict) -> list[dict] | dict:
        # * "*" asterisk represent exclude all but source
        if "*" in metadata.exclude:
            excluded = {metadata.source: data[metadata.source]}
        else:
            excluded = {k: v for k, v in data.items() if k not in metadata.exclude}

        if metadata.source != "*":
            mdata = excluded[metadata.source]
        else:
            mdata = excluded

        return mdata

    def _insert(self, mdata: list[dict] | dict, metadata: PgMetadata):
        if isinstance(mdata, list):
            self.db.ingest(mdata, len(mdata), metadata.table_name, metadata.schema_name)
        elif isinstance(mdata, dict):
            self.db.ingest([mdata], 1, metadata.table_name, metadata.schema_name)
        elif mdata is None:
            return
        else:
            raise Exception("invalid data, must be list[dict] or dict")

    def _update(self, mdata: list[dict] | dict, metadata: PgMetadata):
        if metadata.update_metadata is None:
            raise Exception("update_metadata is null, provide metadata")

        if isinstance(mdata, list):
            for md in mdata:
                self.db.update_rows(
                    table=metadata.table_name,
                    schema=metadata.schema_name,
                    condition=metadata.update_metadata.condition,
                    condition_values={k: md[k] for k in metadata.update_metadata.condition_values},
                    update_values={k: md[k] for k in metadata.update_metadata.updated_values},
                )
        elif isinstance(mdata, dict):
            self.db.update_rows(
                table=metadata.table_name,
                schema=metadata.schema_name,
                condition=metadata.update_metadata.condition,
                condition_values={k: mdata[k] for k in metadata.update_metadata.condition_values},
                update_values={k: mdata[k] for k in metadata.update_metadata.updated_values},
            )
        elif mdata is None:
            return
        else:
            raise Exception("invalid data, must be list[dict] or dict")

    def update(self, subject: DataSourceSubject):
        data = subject.data

        for metadata in self.metadatas:
            mdata = self._exclude_data(metadata, data)

            if metadata.mode == "insert":
                self._insert(mdata, metadata)
            elif metadata.mode == "update":
                self._update(mdata, metadata)
