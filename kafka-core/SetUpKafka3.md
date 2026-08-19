# <span style="color:hsl(142,68%,32%)">Setting Up Kafka 3.0.0</span>

<details><summary>Mac</summary>
<p>

- Make sure you are navigated inside the bin directory.

## <span style="color:hsl(152,68%,36%)">Start Zookeeper and Kafka Broker</span>

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

## <span style="color:hsl(161,68%,36%)">How to create a topic ?</span>

```
./kafka-topics.sh --create --topic test-topic --replication-factor 1 --partitions 4 --bootstrap-server localhost:9092
```

## <span style="color:hsl(171,68%,36%)">How to instantiate a Console Producer?</span>

### <span style="color:hsl(181,68%,36%)">Without Key</span>

```
./kafka-console-producer.sh --broker-list localhost:9092 --topic test-topic
```

### <span style="color:hsl(191,68%,36%)">With Key</span>

```
./kafka-console-producer.sh --broker-list localhost:9092 --topic test-topic --property "key.separator=-" --property "parse.key=true"
```

## <span style="color:hsl(200,68%,44%)">How to instantiate a Console Consumer?</span>

### <span style="color:hsl(210,68%,44%)">Without Key</span>

```
./kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic test-topic --from-beginning
```

### <span style="color:hsl(220,68%,44%)">With Key</span>

```
./kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic test-topic --from-beginning -property "key.separator= - " --property "print.key=true"
```

### <span style="color:hsl(230,68%,44%)">With Consumer Group</span>

```
./kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic test-topic --group <group-name>
```
</p>

</details>

<details><summary>Windows</summary>
<p>

- Make sure you are inside the **bin/windows** directory.

## <span style="color:hsl(239,68%,44%)">Start Zookeeper and Kafka Broker</span>

-   Start up the Zookeeper.

```
zookeeper-server-start.bat ..\..\config\zookeeper.properties
```

-   Start up the Kafka Broker.

```
kafka-server-start.bat ..\..\config\server.properties
```

## <span style="color:hsl(249,68%,44%)">How to create a topic ?</span>

```
kafka-topics.bat --create --topic test-topic  --replication-factor 1 --partitions 4 --bootstrap-server localhost:9092
```

## <span style="color:hsl(259,68%,44%)">How to instantiate a Console Producer?</span>

### <span style="color:hsl(268,68%,44%)">Without Key</span>

```
kafka-console-producer.bat --broker-list localhost:9092 --topic test-topic
```

### <span style="color:hsl(278,68%,44%)">With Key</span>

```
kafka-console-producer.bat --broker-list localhost:9092 --topic test-topic --property "key.separator=-" --property "parse.key=true"
```

## <span style="color:hsl(288,68%,44%)">How to instantiate a Console Consumer?</span>

### <span style="color:hsl(298,68%,44%)">Without Key</span>

```
kafka-console-consumer.bat --bootstrap-server localhost:9092 --topic test-topic --from-beginning
```

### <span style="color:hsl(307,68%,44%)">With Key</span>

```
kafka-console-consumer.bat --bootstrap-server localhost:9092 --topic test-topic --from-beginning -property "key.separator= - " --property "print.key=true"
```

### <span style="color:hsl(317,68%,44%)">With Consumer Group</span>

```
kafka-console-consumer.bat --bootstrap-server localhost:9092 --topic test-topic --group <group-name>
```
</p>

</details>

## <span style="color:hsl(327,68%,44%)">Setting Up Multiple Kafka Brokers</span>

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

### <span style="color:hsl(337,68%,44%)">Starting up the new Broker</span>

- Provide the new **server.properties** thats added.

```
./kafka-server-start.sh ../config/server-1.properties
```

```
./kafka-server-start.sh ../config/server-2.properties
```

# <span style="color:hsl(346,68%,44%)">Advanced Kafka CLI operations:</span>

<details><summary>Mac</summary>
<p>

## <span style="color:hsl(356,68%,44%)">List the topics in a cluster</span>

```
./kafka-topics.sh --bootstrap-server localhost:9092 --list
```

## <span style="color:hsl(6,68%,44%)">Describe topic</span>

- The below command can be used to describe all the topics.

```
./kafka-topics.sh --bootstrap-server localhost:9092 --describe
```

- The below command can be used to describe a specific topic.

```
./kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic <topic-name>
```

## <span style="color:hsl(16,68%,44%)">Alter the min insync replica</span>
```
./kafka-configs.sh  --bootstrap-server localhost:9092 --entity-type topics --entity-name library-events --alter --add-config min.insync.replicas=2
```

## <span style="color:hsl(25,68%,44%)">Delete a topic</span>

```
./kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic test-topic
```
## <span style="color:hsl(35,68%,44%)">How to view consumer groups</span>

```
./kafka-consumer-groups.sh --bootstrap-server localhost:9092 --list
```

### <span style="color:hsl(45,68%,32%)">Consumer Groups and their Offset</span>

```
./kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group console-consumer-27773
```

## <span style="color:hsl(54,68%,32%)">Viewing the Commit Log</span>

```
./kafka-run-class.sh kafka.tools.DumpLogSegments --deep-iteration --files /tmp/kafka-logs/test-topic-0/00000000000000000000.log
```

## <span style="color:hsl(64,68%,32%)">Setting the Minimum Insync Replica</span>

```
./kafka-configs.sh --alter --bootstrap-server localhost:9092 --entity-type topics --entity-name test-topic --add-config min.insync.replicas=2
```
</p>
</details>


<details><summary>Windows</summary>
<p>

- Make sure you are inside the **bin/windows** directory.

## <span style="color:hsl(74,68%,32%)">List the topics in a cluster</span>

```
kafka-topics.bat --bootstrap-server localhost:9092 --list
```

## <span style="color:hsl(84,68%,32%)">Describe topic</span>

- The below command can be used to describe all the topics.

```
kafka-topics.bat --bootstrap-server localhost:9092 --describe
```

- The below command can be used to describe a specific topic.

```
kafka-topics.bat --bootstrap-server localhost:9092 --describe --topic <topic-name>
```

## <span style="color:hsl(93,68%,32%)">Alter the min insync replica</span>
```
kafka-configs.bat --bootstrap-server localhost:9092 --entity-type topics --entity-name library-events --alter --add-config min.insync.replicas=2
```


## <span style="color:hsl(103,68%,32%)">Delete a topic</span>

```
kafka-topics.bat --bootstrap-server localhost:9092 --delete --topic <topic-name>
```


## <span style="color:hsl(113,68%,32%)">How to view consumer groups</span>

```
kafka-consumer-groups.bat --bootstrap-server localhost:9092 --list
```

### <span style="color:hsl(123,68%,32%)">Consumer Groups and their Offset</span>

```
kafka-consumer-groups.bat --bootstrap-server localhost:9092 --describe --group console-consumer-27773
```

## <span style="color:hsl(132,68%,32%)">Viewing the Commit Log</span>

```
kafka-run-class.bat kafka.tools.DumpLogSegments --deep-iteration --files /tmp/kafka-logs/test-topic-0/00000000000000000000.log
```
</p>
</details>
