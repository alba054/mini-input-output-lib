from loguru import logger
from pydantic import ValidationError
from spengine.app.s3 import S3
from spengine.base.observer import BaseObserver
from spengine.core.encoder import Encoder
from spengine.data_source.data_source_subject import DataSourceSubject
from spengine.model.s3_metadata import S3Metadata


class S3TargetOutput(BaseObserver):

    # the order of processor matters
    # because processor will be called in the order of the list
    def __init__(self, s3_client: S3):
        super().__init__()
        self._s3_client = s3_client

    def update(self, subject: DataSourceSubject):
        data = subject.data

        if isinstance(data, list):
            for d in data:
                try:
                    if self._validate_metadata(d):
                        b = Encoder.encode(d["data"], d["encoding"])
                        self._s3_client.put_object(d["filename"], b)
                        logger.info(f"success upload {d['filename']}")
                    else:
                        raise Exception("metadata s3 not correct")
                except Exception as e:
                    logger.error(e)
        else:
            try:
                if self._validate_metadata(data):
                    b = Encoder.encode(data["data"], data["encoding"])
                    self._s3_client.put_object(data["filename"], b)
                    logger.info(f"success upload {data['filename']}")
                else:
                    raise Exception("metadata s3 not correct")
            except Exception as e:
                logger.error(e)

    def _validate_metadata(self, metadata: dict) -> bool:
        try:
            S3Metadata(**metadata)

            return True
        except ValidationError as _:
            return False
