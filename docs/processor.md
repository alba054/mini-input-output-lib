# Processor
**Overview**  
  Processor is what we call transformer process in the pipeline (but sometimes it just filtering the data without altering anything).

## 1. Filter Processor
**Overview**  
Filter processor is a boolean operator. It receives data from input, filter it out and return null if filter is false or return the data if filter is true.
```json
"processors": [
    {
      "type": "filter",
      "components": {
        "operators": {
          "operator": {
            "op": "or",
            "value1": {
              "operator": {
                "op": "in",
                "value1": {
                  "pattern": "in_reply_to_username",
                  "value": null
                },
                "value2": {
                  "pattern": null,
                  "value": [
                    "kemenkeu",
                    "kemenkeuri",
                    "KemenkeuRI",
                    "Kemenkeuri",
                    "Kemenkeu RI",
                    "smindrawati"
                  ]
                }
              }
            },
            "value2": {
              "operator": {
                "op": "in",
                "value1": {
                  "pattern": "parent_username",
                  "value": null
                },
                "value2": {
                  "pattern": null,
                  "value": [
                    "kemenkeu",
                    "kemenkeuri",
                    "KemenkeuRI",
                    "Kemenkeuri",
                    "Kemenkeu RI",
                    "smindrawati"
                  ]
                }
              }
            }
          }
        }
      }
    }
  ]
```

### Components
**Overview**  
Filter processor must have *operators* field which contain 1 operator and 2 values (2 nodes) left value and right value.

#### Operator
**Overview**  
Operator is a core component on filter process that contain *op*, *value1* and *value2*.

1. *op* can be **eq**, **gt**, **gte**, **lt**, **lte**, and **in** [more](/processor/operator.md)
2. value1 and value2 (left and right correspondingly) will have the same properties [more](/processor/value.md)

## 2. Mapping Processor
**Overview**  
Mapping processor is as what it is called, map/transform the data (list[dict] or dict) from input to expected format (return as list[dict] or dict as well). Sometimes any field(s) resulted from this processor can be used as an additional info for output e.g
```json
"pg_info_": {
  "on_update_excluded_cols": [] # what field not to include when updating via on conflict update
}
```