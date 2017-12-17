package org.behrang.examples.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * A very simple Kafka producer.
 */
public class VerySimpleProducer {

    // The topic we are going to write records to
    private static final String KAFKA_TOPIC_NAME = "very_simple_topic";

    public static void main(String[] args) throws InterruptedException {
        // Set producer configuration properties
        final Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        // Continuously send records to the topic
        try (final KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps)) {
            int counter = 1;
            while (true) {
                final String key = "key-" + counter;
                final String value = "value-" + counter;

                System.out.printf("Sending (%s, %s)\n", key, value);

                producer.send(new ProducerRecord<>(KAFKA_TOPIC_NAME, key, value));
                counter++;
                Thread.sleep(50);
            }
        }
    }
}