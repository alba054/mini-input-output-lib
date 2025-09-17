# Value
**Overview**  
value consist of 3 forms, literal value, jmespath pattern, and another operator ([reference here](/processor.md))

## Literal Value
literal value is placed on *value* property
```json
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
```

## Jmespath Pattern
pattern is placed on *pattern* property
```json
"value2": {
  "pattern": "data.title",
  "value": null
}
```

## Operator
Operator is an operator
```json
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
```