from spengine.components.output_factory import OutputFactory
from spengine.components.processor_factory import ProcessorFactory
from spengine.components.source_factory import SourceFactory
from spengine.data_source.data_source_subject import DataSourceSubject
from spengine.processor.base_processor import BaseProcessor


def build(config: dict):
    try:
        # build processors
        processors: list[BaseProcessor] = []
        for v in config["processors"]:
            processors.append(ProcessorFactory.build(v))

        # build source
        source: DataSourceSubject = None
        source = SourceFactory.build(config["input"], processors=processors)

        # build output
        outputs: list[OutputFactory] = []
        for v in config["output"]:
            outputs.append(OutputFactory.build(v))

        for output in outputs:
            source.attach(output)

        source.notify()
    except:
        raise
