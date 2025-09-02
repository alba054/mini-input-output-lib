import json
from spengine.base.observer import BaseObserver
from spengine.data_source.data_source_subject import DataSourceSubject


class FileTargetOutput(BaseObserver):
    # the order of processor matters
    # because processor will be called in the order of the list
    def __init__(self):
        super().__init__()

    def update(self, subject: DataSourceSubject):
        data = subject.data

        with open("output_file.json", "w") as writer:
            json.dump(data, writer)
