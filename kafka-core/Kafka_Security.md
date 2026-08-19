# <span style="color:hsl(180,80%,58%)">Enabling SSL in Kafka</span>

- Follow the below steps for enabling SSL in your local environment

## <span style="color:hsl(318,80%,58%)">Generating the KeyStore</span>

- The below command is to generate the **keyStore**.
- KeyStore in general has information about the server and the organization

```
keytool -keystore server.keystore.jks -alias localhost -validity 365 -genkey -keyalg RSA
```

**Example**  
- After entering all the details the final value will look like below.

```
CN=localhost, OU=localhost, O=localhost, L=Chennai, ST=TN, C=IN
```

## <span style="color:hsl(95,80%,58%)">Generating CA</span>

- The below command will generate the ca cert(SSL cert) and private key. This is normally needed if we are self signing the request.

```
openssl req -new -x509 -keyout ca-key -out ca-cert -days 365 -subj "/CN=local-security-CA"
```

## <span style="color:hsl(233,80%,58%)">Certificate Signing Request(CSR)</span>

- The below command will create a **cert-file** as a result of executing the command.

```
keytool -keystore server.keystore.jks -alias localhost -certreq -file cert-file
```

## <span style="color:hsl(10,80%,58%)">Signing the certificate</span>

- The below command takes care of signing the CSR and then it spits out a file **cert-signed**

```
openssl x509 -req -CA ca-cert -CAkey ca-key -in cert-file -out cert-signed -days 365 -CAcreateserial -passin pass:password
```

- To view the content inside the file **cert-signed**, run the below command.

```
keytool -printcert -v -file cert-signed
```


## <span style="color:hsl(148,80%,58%)">Adding the Signed Cert in to the KeyStore file</span>

```
keytool -keystore server.keystore.jks -alias CARoot -import -file ca-cert
keytool -keystore server.keystore.jks -alias localhost -import -file cert-signed
```

## <span style="color:hsl(285,80%,58%)">Generate the TrustStore</span>

- The below command takes care of generating the truststore for us and adds the **CA-Cert** in to it.
- This is to make sure the client is going to trust all the certs issued by CA.

```
keytool -keystore client.truststore.jks -alias CARoot -import -file ca-cert
```

## <span style="color:hsl(63,80%,50%)">Broker SSL Settings</span>

```
ssl.keystore.location=<location>/server.keystore.jks
ssl.keystore.password=password
ssl.key.password=password
ssl.endpoint.identification.algorithm=
```
# <span style="color:hsl(200,80%,58%)">Accessing SSL Enabled Topics using Console Producers/Consumers</span>

- Create a topic

```
./kafka-topics.sh --create --topic test-topic -zookeeper localhost:2181 --replication-factor 1 --partitions 3
```

- Create a file named **client-ssl.properties** and have the below properties configured in there.

```
security.protocol=SSL
ssl.truststore.location=<location>/client.truststore.jks
ssl.truststore.password=password
ssl.truststore.type=JKS
```

## <span style="color:hsl(338,80%,58%)">Producing Messages to Secured Topic</span>

- Command to Produce Messages to the secured topic

```
./kafka-console-producer.sh --broker-list localhost:9095,localhost:9096,localhost:9097 --topic test-topic --producer.config client-ssl.properties
```

## <span style="color:hsl(115,80%,58%)">Consuming Messages from a Secured Topic</span>

- Command to Produce Messages to the secured topic

```
./kafka-console-consumer.sh --bootstrap-server localhost:9095,localhost:9096,localhost:9097 --topic test-topic --consumer.config client-ssl.properties
```


## <span style="color:hsl(253,80%,58%)">Producing Messages to Non-Secured Topic</span>

```
./kafka-console-producer.sh --broker-list localhost:9092,localhost:9093,localhost:9094 --topic test-topic
```


## <span style="color:hsl(30,80%,58%)">Consuming Messages from a Non-Secured Topic</span>

```
./kafka-console-consumer.sh --bootstrap-server localhost:9092,localhost:9093,localhost:9094 --topic test-topic
```

## <span style="color:hsl(168,80%,58%)">2 Way Authentication</span>

- This config is to enable the client authentication at the cluster end.

```
keytool -keystore server.truststore.jks -alias CARoot -import -file ca-cert
```

- Add the **ssl.client.auth** property in the **server.properties**.

```
ssl.truststore.location=<location>/server.truststore.jks
ssl.truststore.password=password
ssl.client.auth=required
```
- Kafka Client should have the following the config in the **client-ssl.properties** file

```
ssl.keystore.type=JKS
ssl.keystore.location=<location>/client.keystore.jks
ssl.keystore.password=password
ssl.key.password=password
```
