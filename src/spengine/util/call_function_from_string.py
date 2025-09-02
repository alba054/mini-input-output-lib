import os
import sys
from importlib import import_module


def call_function_from_string(func_string):
    module_name, function_name = func_string.rsplit(".", 1)

    # Adjust the module path to the current working directory
    base_path = os.getcwd()
    sys.path.insert(0, base_path)

    module = import_module(module_name)
    func = getattr(module, function_name)

    return func
