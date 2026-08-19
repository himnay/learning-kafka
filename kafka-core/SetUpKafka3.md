# <span style="color:hsl(142,80%,58%)">Setting Up Kafka 3.0.0</span>

<details><summary>Mac</summary>
<p>

- Make sure you are navigated inside the bin directory.

## <span style="color:hsl(280,80%,58%)">Start Zookeeper and Kafka Broker</span>

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

## <span style="color:hsl(57,80%,50%)">How to create a topic ?</span>

```
./kafka-topics.sh --create --topic test-topic --replication-factor 1 --partitions 4 --bootstrap-server localhost:9092
```

## <span style="color:hsl(195,80%,58%)">How to instantiate a Console Producer?</span>

### <span style="color:hsl(332,80%,58%)">Without Key</span>

```
./kafka-console-producer.sh --broker-list localhost:9092 --topic test-topic
```

### <span style="color:hsl(110,80%,58%)">With Key</span>

```
./kafka-console-producer.sh --broker-list localhost:9092 --topic test-topic --property "key.separator=-" --property "parse.key=true"
```

## <span style="color:hsl(247,80%,58%)">How to instantiate a Console Consumer?</span>

### <span style="color:hsl(25,80%,58%)">Without Key</span>

```
./kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic test-topic --from-beginning
```

### <span style="color:hsl(162,80%,58%)">With Key</span>

```
./kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic test-topic --from-beginning -property "key.separator= - " --property "print.key=true"
```

### <span style="color:hsl(300,80%,58%)">With Consumer Group</span>

```
./kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic test-topic --group <group-name>
```
</p>

</details>

<details><summary>Windows</summary>
<p>

- Make sure you are inside the **bin/windows** directory.

## <span style="color:hsl(77,80%,58%)">Start Zookeeper and Kafka Broker</span>

-   Start up the Zookeeper.

```
zookeeper-server-start.bat ..\..\config\zookeeper.properties
```

-   Start up the Kafka Broker.

```
kafka-server-start.bat ..\..\config\server.properties
```

## <span style="color:hsl(215,80%,58%)">How to create a topic ?</span>

```
kafka-topics.bat --create --topic test-topic  --replication-factor 1 --partitions 4 --bootstrap-server localhost:9092
```

## <span style="color:hsl(352,80%,58%)">How to instantiate a Console Producer?</span>

### <span style="color:hsl(130,80%,58%)">Without Key</span>

```
kafka-console-producer.bat --broker-list localhost:9092 --topic test-topic
```

### <span style="color:hsl(267,80%,58%)">With Key</span>

```
kafka-console-producer.bat --broker-list localhost:9092 --topic test-topic --property "key.separator=-" --property "parse.key=true"
```

## <span style="color:hsl(45,80%,58%)">How to instantiate a Console Consumer?</span>

### <span style="color:hsl(182,80%,58%)">Without Key</span>

```
kafka-console-consumer.bat --bootstrap-server localhost:9092 --topic test-topic --from-beginning
```

### <span style="color:hsl(320,80%,58%)">With Key</span>

```
kafka-console-consumer.bat --bootstrap-server localhost:9092 --topic test-topic --from-beginning -property "key.separator= - " --property "print.key=true"
```

### <span style="color:hsl(97,80%,58%)">With Consumer Group</span>

```
kafka-console-consumer.bat --bootstrap-server localhost:9092 --topic test-topic --group <group-name>
```
</p>

</details>

## <span style="color:hsl(235,80%,58%)">Setting Up Multiple Kafka Brokers</span>

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

### <span style="color:hsl(12,80%,58%)">Starting up the new Broker</span>

- Provide the new **server.properties** thats added.

```
./kafka-server-start.sh ../config/server-1.properties
```

```
./kafka-server-start.sh ../config/server-2.properties
```

# <span style="color:hsl(150,80%,58%)">Advanced Kafka CLI operations:</span>

<details><summary>Mac</summary>
<p>

## <span style="color:hsl(287,80%,58%)">List the topics in a cluster</span>

```
./kafka-topics.sh --bootstrap-server localhost:9092 --list
```

## <span style="color:hsl(65,80%,50%)">Describe topic</span>

- The below command can be used to describe all the topics.

```
./kafka-topics.sh --bootstrap-server localhost:9092 --describe
```

- The below command can be used to describe a specific topic.

```
./kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic <topic-name>
```

## <span style="color:hsl(202,80%,58%)">Alter the min insync replica</span>
```
./kafka-configs.sh  --bootstrap-server localhost:9092 --entity-type topics --entity-name library-events --alter --add-config min.insync.replicas=2
```

## <span style="color:hsl(340,80%,58%)">Delete a topic</span>

```
./kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic test-topic
```
## <span style="color:hsl(117,80%,58%)">How to view consumer groups</span>

```
./kafka-consumer-groups.sh --bootstrap-server localhost:9092 --list
```

### <span style="color:hsl(255,80%,58%)">Consumer Groups and their Offset</span>

```
./kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group console-consumer-27773
```

## <span style="color:hsl(32,80%,58%)">Viewing the Commit Log</span>

```
./kafka-run-class.sh kafka.tools.DumpLogSegments --deep-iteration --files /tmp/kafka-logs/test-topic-0/00000000000000000000.log
```

## <span style="color:hsl(170,80%,58%)">Setting the Minimum Insync Replica</span>

```
./kafka-configs.sh --alter --bootstrap-server localhost:9092 --entity-type topics --entity-name test-topic --add-config min.insync.replicas=2
```
</p>
</details>


<details><summary>Windows</summary>
<p>

- Make sure you are inside the **bin/windows** directory.

## <span style="color:hsl(307,80%,58%)">List the topics in a cluster</span>

```
kafka-topics.bat --bootstrap-server localhost:9092 --list
```

## <span style="color:hsl(85,80%,58%)">Describe topic</span>

- The below command can be used to describe all the topics.

```
kafka-topics.bat --bootstrap-server localhost:9092 --describe
```

- The below command can be used to describe a specific topic.

```
kafka-topics.bat --bootstrap-server localhost:9092 --describe --topic <topic-name>
```

## <span style="color:hsl(222,80%,58%)">Alter the min insync replica</span>
```
kafka-configs.bat --bootstrap-server localhost:9092 --entity-type topics --entity-name library-events --alter --add-config min.insync.replicas=2
```


## <span style="color:hsl(360,80%,58%)">Delete a topic</span>

```
kafka-topics.bat --bootstrap-server localhost:9092 --delete --topic <topic-name>
```


## <span style="color:hsl(137,80%,58%)">How to view consumer groups</span>

```
kafka-consumer-groups.bat --bootstrap-server localhost:9092 --list
```

### <span style="color:hsl(275,80%,58%)">Consumer Groups and their Offset</span>

```
kafka-consumer-groups.bat --bootstrap-server localhost:9092 --describe --group console-consumer-27773
```

## <span style="color:hsl(52,80%,50%)">Viewing the Commit Log</span>

```
kafka-run-class.bat kafka.tools.DumpLogSegments --deep-iteration --files /tmp/kafka-logs/test-topic-0/00000000000000000000.log
```
</p>
</details>
