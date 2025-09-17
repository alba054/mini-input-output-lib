# 📘 Simple ETL Library – Technical Documentation

Repository: [mini-input-output-lib](https://github.com/alba054/mini-input-output-lib)

---

## 1. Introduction

- **Overview**  
  A lightweight ETL library designed to simplify data extraction, transformation, and loading (ETL) tasks.  

- **Use Cases**  
  - Reduce boilerplate for input and output handling in Python.  
  - Quickly set up streaming or batch pipelines.  
  - Easily connect a single input to multiple outputs with minimal code.  

- **Architecture Summary**  
Input → Processors → Outputs
---

## 2. Getting Started

### Prerequisites
- Python **3.10+** (tested with Python 3.10)

### Installation
Install directly from GitHub release:
```bash
pip install https://github.com/alba054/mini-input-output-lib/releases/download/v1.2.0a/spengine-1.2.0a0-py3-none-any.whl
```

### Minimal Config
```json
{
  "input": {
    "source": "file",
    "metadata": {
      "filepath": "input.json"
    }
  },
  "processors": [
    {
      "type": "mapper",
      "components": {
        "mappers": [
          {
            "object": "mapper.create_mapper",
            "includeInField": "*"
          }
        ]
      }
    }
  ],
  "output": [
    {
      "target": "file",
      "metadata": {
        "filepath": "output.json"
      }
    }
  ]
}
```

*note: refer to processor for the mapper*

## 4. Core API

### [Input](/input)
### [Processor](/processor)