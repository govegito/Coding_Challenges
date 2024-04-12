# Building a Custom In-Memory Messaging Queue

For detailed implementation and ideation journey please go through my [medium article](https://medium.com/@jayeshchouhan826/from-concept-to-code-developing-a-custom-in-memory-messaging-queue-f9c6ee54a3ca).

## Usage:

### Producing messages:


* Our queue is using a pull based mechanism for reading out the messages.
* The producer initiates by registering the topic for which it intends to push messages. Subsequently, it connects to our STOMP server, where it can submit a message via the path “/producer/publish/{topic_name}”.
The message format for pushing into the queue follows this pattern:
    ```json
    {
    "messageID": string,
    "key": string,
    "message": string,
    "timestamp": timestamp
    }
    ```
* Additionally, producers can subscribe to the path “/topic/acknowledgment/{topic_name}” to receive confirmation of message receipt and queue storage.

* For producer flow i have created a sample python script that will keep on generating new messages and will send it to the queue.
Run this command in producers/producer1
    ``` shell
    python producer.py "topci_name" "delay_time_for_sending"
    ```
  
* In the application.properties file i have passed some parameters like, number of brokers, number of partition per topic. You can change as per your need.

### Consuming messages:

* First up, consumers will need to get themselves registered. But before that, we’ve got to ensure the consumer group is set up too.
* Registering Consumer Group
* Registering Consumer
* Subscribing to Topic
* I have also created a python script for consumers too in the same folder as producer. you can fire it using this command:
    ```shell
    python consumer.py "consumer_name" "consumer_group_name" "topic_to_subsribe" "delay_time"
    ```
* Also consumer group will rebalance once the it discovers that consumer has not sent a hearbeat in last 30 seconds or so. You can change the time period for it as per you need as well.

Building from scratch offers a deeply enriching experience, honing your skills in design pattern, scalability, and crafting modular, scalable code. Embrace it fully, for it’s through these challenges that you’ll grow as a developer and truly enjoy the journey.
