package org.behrang.examples.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.*;

import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;

/**
 * A basic multi-threaded Kafka consumer
 */
public class BasicMultithreadedConsumer {

    // The topic we are going to read records from
    private static final String KAFKA_TOPIC_NAME = "topic_with_20_partitions";

    private static final int PARTITION_COUNT = 20;

    public static void main(String[] args) throws InterruptedException {
        // Set consumer configuration properties
        final Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "basic-multithreaded-consumer");

        final List<Thread> partitionConsumers = new ArrayList<>();
        for (int i = 0; i < PARTITION_COUNT; i++) {
            partitionConsumers.add(new Thread(new PartitionConsumer(i, consumerProps)));
        }

        partitionConsumers.forEach(Thread::start);

        for (Thread t : partitionConsumers) {
            t.join();
        }
    }

    private static class PartitionConsumer implements Runnable {

        private final int consumerNumber;

        private final Properties properties;

        public PartitionConsumer(final int consumerNumber, final Properties properties) {
            this.consumerNumber = consumerNumber;
            this.properties = properties;
        }

        @Override
        public void run() {
            // Create a new consumer
            try (final KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties)) {
                // Subscribe to the topic's consumerNumber
                consumer.subscribe(singleton(KAFKA_TOPIC_NAME));

                // Continuously read records from the topic
                while (true) {
                    final ConsumerRecords<String, String> records = consumer.poll(1000);
                    for (ConsumerRecord<String, String> record : records) {
                        System.out.printf("Consumer #%s (assignment: %s) received: %s\n", consumerNumber, consumer.assignment(), record);
                    }
                }
            }
        }
    }

}
