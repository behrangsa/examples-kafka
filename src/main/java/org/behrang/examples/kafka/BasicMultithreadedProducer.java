package org.behrang.examples.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * A very simple Kafka producer.
 */
public class BasicMultithreadedProducer {

    // The topic we are going to write records to
    private static final String KAFKA_TOPIC_NAME = "topic_with_20_partitions";

    private static final int PARTITION_COUNT = 20;

    public static void main(String[] args) throws InterruptedException {
        // Set producer configuration properties
        final Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        final List<Thread> partitionProducers = new ArrayList<>();
        for (int i = 0; i < PARTITION_COUNT; i++) {
            partitionProducers.add(new Thread(new PartitionProducer(i, producerProps)));
        }

        partitionProducers.forEach(Thread::start);

        for (Thread t : partitionProducers) {
            t.join();
        }
    }

    private static class PartitionProducer implements Runnable {

        private final int partition;

        private final Properties properties;

        public PartitionProducer(final int partition, final Properties properties) {
            this.partition = partition;
            this.properties = properties;
        }

        @Override
        public void run() {
            // Continuously send records to the topic
            try (final KafkaProducer<String, String> producer = new KafkaProducer<>(properties)) {
                int counter = 1;
                while (true) {
                    final String key = "key-" + counter;
                    final String value = "value-" + counter;

                    System.out.printf("Sending (%s, %s) from producer (%s)\n", key, value, partition);

                    producer.send(new ProducerRecord<>(KAFKA_TOPIC_NAME, partition, key, value));
                    counter++;

                    try {
                        Thread.sleep(50);
                    } catch (InterruptedException e) {
                        // ignore e
                    }
                }
            }
        }
    }
}