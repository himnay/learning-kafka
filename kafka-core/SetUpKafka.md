# <span style="color:hsl(192,68%,36%)">Setting Up Kafka</span>

<details><summary>Mac</summary>
<p>

- Make sure you are navigated inside the bin directory.

## <span style="color:hsl(202,68%,44%)">Start Zookeeper and Kafka Broker</span>

-   Start up the Zookeeper.

```
./zookeeper-server-start.sh ../config/zookeeper.properties
```

- Add the below properties in the server.properties

```
listeners=PLAINTEXT://localhost:9092
auto.create.topics.enable=false
```

-   Start up the Kafka Broker

```
./kafka-server-start.sh ../config/server.properties
```

## <span style="color:hsl(211,68%,44%)">How to create a topic ?</span>

```
./kafka-topics.sh --create --topic test-topic -zookeeper localhost:2181 --replication-factor 1 --partitions 4
```

## <span style="color:hsl(221,68%,44%)">How to instantiate a Console Producer?</span>

### <span style="color:hsl(231,68%,44%)">Without Key</span>

```
./kafka-console-producer.sh --broker-list localhost:9092 --topic test-topic
```

### <span style="color:hsl(241,68%,44%)">With Key</span>

```
./kafka-console-producer.sh --broker-list localhost:9092 --topic test-topic --property "key.separator=-" --property "parse.key=true"
```

## <span style="color:hsl(250,68%,44%)">How to instantiate a Console Consumer?</span>

### <span style="color:hsl(260,68%,44%)">Without Key</span>

```
./kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic test-topic --from-beginning
```

### <span style="color:hsl(270,68%,44%)">With Key</span>

```
./kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic test-topic --from-beginning -property "key.separator= - " --property "print.key=true"
```

### <span style="color:hsl(280,68%,44%)">With Consumer Group</span>

```
./kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic test-topic --group <group-name>
```
</p>

</details>

<details><summary>Windows</summary>
<p>

- Make sure you are inside the **bin/windows** directory.

## <span style="color:hsl(289,68%,44%)">Start Zookeeper and Kafka Broker</span>

-   Start up the Zookeeper.

```
zookeeper-server-start.bat ..\..\config\zookeeper.properties
```

-   Start up the Kafka Broker.

```
kafka-server-start.bat ..\..\config\server.properties
```

## <span style="color:hsl(299,68%,44%)">How to create a topic ?</span>

```
kafka-topics.bat --create --topic test-topic -zookeeper localhost:2181 --replication-factor 1 --partitions 4
```

## <span style="color:hsl(309,68%,44%)">How to instantiate a Console Producer?</span>

### <span style="color:hsl(318,68%,44%)">Without Key</span>

```
kafka-console-producer.bat --broker-list localhost:9092 --topic test-topic
```

### <span style="color:hsl(328,68%,44%)">With Key</span>

```
kafka-console-producer.bat --broker-list localhost:9092 --topic test-topic --property "key.separator=-" --property "parse.key=true"
```

## <span style="color:hsl(338,68%,44%)">How to instantiate a Console Consumer?</span>

### <span style="color:hsl(348,68%,44%)">Without Key</span>

```
kafka-console-consumer.bat --bootstrap-server localhost:9092 --topic test-topic --from-beginning
```

### <span style="color:hsl(357,68%,44%)">With Key</span>

```
kafka-console-consumer.bat --bootstrap-server localhost:9092 --topic test-topic --from-beginning -property "key.separator= - " --property "print.key=true"
```

### <span style="color:hsl(7,68%,44%)">With Consumer Group</span>

```
kafka-console-consumer.bat --bootstrap-server localhost:9092 --topic test-topic --group <group-name>
```
</p>

</details>

## <span style="color:hsl(17,68%,44%)">Setting Up Multiple Kafka Brokers</span>

- The first step is to add a new **server.properties**.

- We need to modify three properties to start up a multi broker set up.

```
broker.id=<unique-broker-d>
listeners=PLAINTEXT://localhost:<unique-port>
log.dirs=/tmp/<unique-kafka-folder>
auto.create.topics.enable=false
```

- Example config will be like below.

```
broker.id=1
listeners=PLAINTEXT://localhost:9093
log.dirs=/tmp/kafka-logs-1
auto.create.topics.enable=false
```

### <span style="color:hsl(27,68%,44%)">Starting up the new Broker</span>

- Provide the new **server.properties** thats added.

```
./kafka-server-start.sh ../config/server-1.properties
```

```
./kafka-server-start.sh ../config/server-2.properties
```

# <span style="color:hsl(36,68%,44%)">Advanced Kafka CLI operations:</span>

<details><summary>Mac</summary>
<p>

## <span style="color:hsl(46,68%,32%)">List the topics in a cluster</span>

```
./kafka-topics.sh --zookeeper localhost:2181 --list
```

## <span style="color:hsl(56,68%,32%)">Describe topic</span>

- The below command can be used to describe all the topics.

```
./kafka-topics.sh --zookeeper localhost:2181 --describe
```

- The below command can be used to describe a specific topic.

```
./kafka-topics.sh --zookeeper localhost:2181 --describe --topic <topic-name>
```

## <span style="color:hsl(66,68%,32%)">Alter the min insync replica</span>
```
./kafka-topics.sh --alter --zookeeper localhost:2181 --topic library-events --config min.insync.replicas=2
```

## <span style="color:hsl(75,68%,32%)">Delete a topic</span>

```
./kafka-topics.sh --zookeeper localhost:2181 --delete --topic test-topic
```
## <span style="color:hsl(85,68%,32%)">How to view consumer groups</span>

```
./kafka-consumer-groups.sh --bootstrap-server localhost:9092 --list
```

### <span style="color:hsl(95,68%,32%)">Consumer Groups and their Offset</span>

```
./kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group console-consumer-27773
```

## <span style="color:hsl(104,68%,32%)">Viewing the Commit Log</span>

```
./kafka-run-class.sh kafka.tools.DumpLogSegments --deep-iteration --files /tmp/kafka-logs/test-topic-0/00000000000000000000.log
```

## <span style="color:hsl(114,68%,32%)">Setting the Minimum Insync Replica</span>

```
./kafka-configs.sh --alter --zookeeper localhost:2181 --entity-type topics --entity-name test-topic --add-config min.insync.replicas=2
```
</p>
</details>


<details><summary>Windows</summary>
<p>

- Make sure you are inside the **bin/windows** directory.

## <span style="color:hsl(124,68%,32%)">List the topics in a cluster</span>

```
kafka-topics.bat --zookeeper localhost:2181 --list
```

## <span style="color:hsl(134,68%,32%)">Describe topic</span>

- The below command can be used to describe all the topics.

```
kafka-topics.bat --zookeeper localhost:2181 --describe
```

- The below command can be used to describe a specific topic.

```
kafka-topics.bat --zookeeper localhost:2181 --describe --topic <topic-name>
```

## <span style="color:hsl(143,68%,32%)">Alter the min insync replica</span>
```
kafka-topics.bat --alter --zookeeper localhost:2181 --topic library-events --config min.insync.replicas=2
```


## <span style="color:hsl(153,68%,36%)">Delete a topic</span>

```
kafka-topics.bat --zookeeper localhost:2181 --delete --topic <topic-name>
```


## <span style="color:hsl(163,68%,36%)">How to view consumer groups</span>

```
kafka-consumer-groups.bat --bootstrap-server localhost:9092 --list
```

### <span style="color:hsl(173,68%,36%)">Consumer Groups and their Offset</span>

```
kafka-consumer-groups.bat --bootstrap-server localhost:9092 --describe --group console-consumer-27773
```

## <span style="color:hsl(182,68%,36%)">Viewing the Commit Log</span>

```
kafka-run-class.bat kafka.tools.DumpLogSegments --deep-iteration --files /tmp/kafka-logs/test-topic-0/00000000000000000000.log
```
</p>
</details>
