import json
import sys
from spengine.components import builder


def bootstrap():
    if len(sys.argv) < 2:
        raise Exception("provide filepath to config, must be json file")

    filepath = sys.argv[1]

    if filepath.split(".")[-1] != "json":
        raise Exception("configuration file must be json")

    with open(filepath, "r") as reader:
        config = json.load(reader)

        builder.build(config)


def main():
    bootstrap()


if __name__ == "__main__":
    main()
