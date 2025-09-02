import json
from spengine.data_source.data_source_subject import DataSourceSubject
from spengine.processor.base_processor import BaseProcessor


class FileSubject(DataSourceSubject):
    def __init__(self, filepath: str, processors: list[BaseProcessor]):
        super().__init__(processors=processors)

        with open(filepath, "r") as reader:
            self.data = json.load(reader)
            self.additional_info = {"kafka_timestamp": 1756357283000}
            for processor in self.processors:
                self.data = processor.process(data=self.data, additional_info=self.additional_info)
