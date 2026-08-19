# <span style="color:hsl(192,80%,58%)">Setting Up Kafka</span>

<details><summary>Mac</summary>
<p>

- Make sure you are navigated inside the bin directory.

## <span style="color:hsl(330,80%,58%)">Start Zookeeper and Kafka Broker</span>

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

## <span style="color:hsl(107,80%,58%)">How to create a topic ?</span>

```
./kafka-topics.sh --create --topic test-topic -zookeeper localhost:2181 --replication-factor 1 --partitions 4
```

## <span style="color:hsl(245,80%,58%)">How to instantiate a Console Producer?</span>

### <span style="color:hsl(22,80%,58%)">Without Key</span>

```
./kafka-console-producer.sh --broker-list localhost:9092 --topic test-topic
```

### <span style="color:hsl(160,80%,58%)">With Key</span>

```
./kafka-console-producer.sh --broker-list localhost:9092 --topic test-topic --property "key.separator=-" --property "parse.key=true"
```

## <span style="color:hsl(297,80%,58%)">How to instantiate a Console Consumer?</span>

### <span style="color:hsl(75,80%,58%)">Without Key</span>

```
./kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic test-topic --from-beginning
```

### <span style="color:hsl(212,80%,58%)">With Key</span>

```
./kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic test-topic --from-beginning -property "key.separator= - " --property "print.key=true"
```

### <span style="color:hsl(350,80%,58%)">With Consumer Group</span>

```
./kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic test-topic --group <group-name>
```
</p>

</details>

<details><summary>Windows</summary>
<p>

- Make sure you are inside the **bin/windows** directory.

## <span style="color:hsl(127,80%,58%)">Start Zookeeper and Kafka Broker</span>

-   Start up the Zookeeper.

```
zookeeper-server-start.bat ..\..\config\zookeeper.properties
```

-   Start up the Kafka Broker.

```
kafka-server-start.bat ..\..\config\server.properties
```

## <span style="color:hsl(265,80%,58%)">How to create a topic ?</span>

```
kafka-topics.bat --create --topic test-topic -zookeeper localhost:2181 --replication-factor 1 --partitions 4
```

## <span style="color:hsl(42,80%,58%)">How to instantiate a Console Producer?</span>

### <span style="color:hsl(180,80%,58%)">Without Key</span>

```
kafka-console-producer.bat --broker-list localhost:9092 --topic test-topic
```

### <span style="color:hsl(317,80%,58%)">With Key</span>

```
kafka-console-producer.bat --broker-list localhost:9092 --topic test-topic --property "key.separator=-" --property "parse.key=true"
```

## <span style="color:hsl(95,80%,58%)">How to instantiate a Console Consumer?</span>

### <span style="color:hsl(232,80%,58%)">Without Key</span>

```
kafka-console-consumer.bat --bootstrap-server localhost:9092 --topic test-topic --from-beginning
```

### <span style="color:hsl(10,80%,58%)">With Key</span>

```
kafka-console-consumer.bat --bootstrap-server localhost:9092 --topic test-topic --from-beginning -property "key.separator= - " --property "print.key=true"
```

### <span style="color:hsl(147,80%,58%)">With Consumer Group</span>

```
kafka-console-consumer.bat --bootstrap-server localhost:9092 --topic test-topic --group <group-name>
```
</p>

</details>

## <span style="color:hsl(285,80%,58%)">Setting Up Multiple Kafka Brokers</span>

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

### <span style="color:hsl(62,80%,50%)">Starting up the new Broker</span>

- Provide the new **server.properties** thats added.

```
./kafka-server-start.sh ../config/server-1.properties
```

```
./kafka-server-start.sh ../config/server-2.properties
```

# <span style="color:hsl(200,80%,58%)">Advanced Kafka CLI operations:</span>

<details><summary>Mac</summary>
<p>

## <span style="color:hsl(337,80%,58%)">List the topics in a cluster</span>

```
./kafka-topics.sh --zookeeper localhost:2181 --list
```

## <span style="color:hsl(115,80%,58%)">Describe topic</span>

- The below command can be used to describe all the topics.

```
./kafka-topics.sh --zookeeper localhost:2181 --describe
```

- The below command can be used to describe a specific topic.

```
./kafka-topics.sh --zookeeper localhost:2181 --describe --topic <topic-name>
```

## <span style="color:hsl(252,80%,58%)">Alter the min insync replica</span>
```
./kafka-topics.sh --alter --zookeeper localhost:2181 --topic library-events --config min.insync.replicas=2
```

## <span style="color:hsl(30,80%,58%)">Delete a topic</span>

```
./kafka-topics.sh --zookeeper localhost:2181 --delete --topic test-topic
```
## <span style="color:hsl(167,80%,58%)">How to view consumer groups</span>

```
./kafka-consumer-groups.sh --bootstrap-server localhost:9092 --list
```

### <span style="color:hsl(305,80%,58%)">Consumer Groups and their Offset</span>

```
./kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group console-consumer-27773
```

## <span style="color:hsl(82,80%,58%)">Viewing the Commit Log</span>

```
./kafka-run-class.sh kafka.tools.DumpLogSegments --deep-iteration --files /tmp/kafka-logs/test-topic-0/00000000000000000000.log
```

## <span style="color:hsl(220,80%,58%)">Setting the Minimum Insync Replica</span>

```
./kafka-configs.sh --alter --zookeeper localhost:2181 --entity-type topics --entity-name test-topic --add-config min.insync.replicas=2
```
</p>
</details>


<details><summary>Windows</summary>
<p>

- Make sure you are inside the **bin/windows** directory.

## <span style="color:hsl(357,80%,58%)">List the topics in a cluster</span>

```
kafka-topics.bat --zookeeper localhost:2181 --list
```

## <span style="color:hsl(135,80%,58%)">Describe topic</span>

- The below command can be used to describe all the topics.

```
kafka-topics.bat --zookeeper localhost:2181 --describe
```

- The below command can be used to describe a specific topic.

```
kafka-topics.bat --zookeeper localhost:2181 --describe --topic <topic-name>
```

## <span style="color:hsl(272,80%,58%)">Alter the min insync replica</span>
```
kafka-topics.bat --alter --zookeeper localhost:2181 --topic library-events --config min.insync.replicas=2
```


## <span style="color:hsl(50,80%,50%)">Delete a topic</span>

```
kafka-topics.bat --zookeeper localhost:2181 --delete --topic <topic-name>
```


## <span style="color:hsl(187,80%,58%)">How to view consumer groups</span>

```
kafka-consumer-groups.bat --bootstrap-server localhost:9092 --list
```

### <span style="color:hsl(325,80%,58%)">Consumer Groups and their Offset</span>

```
kafka-consumer-groups.bat --bootstrap-server localhost:9092 --describe --group console-consumer-27773
```

## <span style="color:hsl(102,80%,58%)">Viewing the Commit Log</span>

```
kafka-run-class.bat kafka.tools.DumpLogSegments --deep-iteration --files /tmp/kafka-logs/test-topic-0/00000000000000000000.log
```
</p>
</details>
