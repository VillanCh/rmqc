# rmqc
RabbitMQ Client with a fixed usage. You should publish to an exchange, one consumer or one message getter should only have one queue.

* To Publish messages to one EXCHANGE.
* To Consume messages in one QUEUE with one or more pairs (EXCHANGE - ROUTING_KEY).
* To Get messages in one QUEUE with one pair (EXCHANGE - ROUTING_KEY).
