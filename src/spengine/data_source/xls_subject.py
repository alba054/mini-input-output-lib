from loguru import logger
import numpy as np
import pandas as pd
from spengine.data_source.data_source_subject import DataSourceSubject
from spengine.processor.base_processor import BaseProcessor


class XlsSubject(DataSourceSubject):
    def __init__(self, filepath: str, processors: list[BaseProcessor]):
        super().__init__(processors=processors)

        self.df = pd.read_excel(filepath)
        self.df = self.df.replace({np.nan: None})

    def notify(self):
        rows = self.df.to_dict("records")
        for message in rows:
            self.data = message

            for processor in self.processors:
                self.data = processor.process(data=self.data)

            for observer in self._service_observers:
                try:
                    observer.update(self)
                except Exception as e:
                    import traceback

                    traceback.print_exc()
                    logger.error(e)
                    continue
