Instructions:

Set Up:

Kafka:https://kafka.apache.org/downloads.html
Flink: https://flink.apache.org/downloads.html

After configuring your cluster with Kafka and Flink, it is necessary to configure:
- One node as a Sender, which will provide (using Kafka) the data (sender).
- One node as a Flink master, which will receive (by kafka) the data and distribute the load to the slave nodes.
- "n" nodes as Flink slaves to process the tasks.

The main classes of our project are:

PRIMEbigdata.PRIMEMain (Proposed technique)

DataProducer.SENDERAttSelection (Sender + Attribute Selection)

PRIMEbigdata.StreamingMetablocking (Streaming Metablocking)


To run the techniques, please, use the following commands in your cluster:

./bin/flink run --class PRIMEbigdata.PRIMEMain /<FILE_NAME>.jar FLINK_HOST ZOOKEEPER_HOST TOP-N WINDOW_SIZE SLICE_SIZE OUTPUT NUMBER_NODES ACTIVE_FILTER FILTER_SIZE

java -jar /<FILE_NAME>.jar KAFKA_HOST TIME_PERIODICY DATA_SOURCE_PATH_1 DATA_SOURCE_PATH_2 GROUNDTRUTH_PATH PERCENTAGE_PER_INCREMENT APPLY_ATTRIBUTE_SELECTION

./bin/flink run --class PRIMEbigdata.StreamingMetablocking /<FILE_NAME>.jar FLINK_HOST ZOOKEEPER_HOST TOP-N WINDOW_SIZE SLICE_SIZE OUTPUT NUMBER_NODES ACTIVE_FILTER FILTER_SIZE


The datasets and groundtruths applied to evaluate the proposed blocking technique are located at the datasets directory.
