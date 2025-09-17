# Input
**Overview**  
Input is where the data's coming e.g file(json and excel supported), kafka, beanstalkd, rabbitmq

## 1. File (json only)
```json
...
"input": {
  "source": "file",
  "metadata": {
    "filepath": "sample_jasa_raharja_new_v2.json"
  }
}
...
```

## 2. Excel (XLSX)
**Overview**  
this input type can only handle xlsx file that has one sheet and valid tables
```json
...
"input": {
  "source": "xls",
  "metadata": {
    "filepath": "input.xlsx"
  }
}
...
```

## 3. Kafka
**Overview**  
this input streams the data from kafka. all error from processors will be ignored and the data is still committed successfully if *auto_commit = true*.
```json
...
"input": {
"source": "kafka",
"metadata": {
  "brokers": ["broker_host"],
  "topic": ["topic1"],
  "offset": "earliest", # earliest or latest
  "commit": true, # enable or disable auto_commit
  "poll": 10, # message to poll 
  "groupId": "groupid1"
  }
}
...
```
processor that receive data from kafka input will have *additional_info*
```json
{
  "kafka_timestamp": kafka_message.timestamp
}
```
*additional_info* access on processor e.g mapper processor 

## 4. Beanstalkd
**Overview**  
this input consume the data from beanstalkd. if one of the processor raise an error then all processors after it will not process anything so that the output will be ommited and the the job will be buried
```json
...
"input": {
  "source": "beanstalk",
  "metadata": {
    "host": "<host>",
    "port": 11300,
    "topic": "<topic>"
  }
}
...
```

## 5. RabbitMQ
**Overview**  
this input consume the data from rabbitmq. if one of the processor raise an error then all processors after it will not process anything so that the output will be ommited and the the job will not be acknowlodged
```json
...
"input": {
  "source": "rabbit",
  "metadata": {
    "host": "<host>",
    "username": "<username>",
    "password": "<password>",
    "port": <port>,
    "exchange": "<exchange>",
    "exchangeType": "<exchangeType>",
    "queueName": "<queueName>",
    "prefetchCount": <how many to fetch for one consumer>,
    "routingKey": "<routingKey>",
    "vhost": "<vhost>",
    "durable": <bool>,
    "tls": <bool> # use tls or not
  }
}
...
```
