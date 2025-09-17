# Op

## EQ
left_value == right_value (any json valid data types)

## GT
left_value > right_value (number)

## GTE
left_value >= right_value (number)

## LT
left_value < right_value (number)

## LTE
left_value <= right_value (number)

## IN
left_value is in right_value

### Example
```
"abc" in "abcabc" # true
```
###
```
"abc" in ["abc", "bca", ....] # true
```