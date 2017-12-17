# A pair of very simple Kafka producer and consumer applications

### Build

```bash
$ mvn clean install
```

### Run the consumer in one terminal window

```bash
$ mvn exec:java -Dexec.mainClass=org.behrang.examples.kafka.BasicMultithreadedConsumer
```

### Run the producer in another terminal

```bash
$ mvn exec:java -Dexec.mainClass=org.behrang.examples.kafka.BasicMultithreadedProducer
```