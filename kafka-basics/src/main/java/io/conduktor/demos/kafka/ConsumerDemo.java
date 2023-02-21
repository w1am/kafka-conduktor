package io.conduktor.demos.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemo {

    private static final Logger log = LoggerFactory.getLogger(ConsumerDemo.class.getSimpleName());

    public static void main(String[] args) {
        log.info("I am a Kafka Consumer!");

        String groupId = "my-java-application";
        String topic = "demo_java";

        // Create producer properties
        Properties properties = new Properties();

        // Connect to localhost
        // properties.setProperty("bootstrap.servers", "127.0.0.1:9092");

        // connect to Conduktor Playground
        properties.setProperty("bootstrap.servers", "cluster.playground.cdkt.io:9092");
        properties.setProperty("security.protocol", "SASL_SSL");
        properties.setProperty("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"7Mbt7B2cnGYWoHApzGwfS5\" password=\"eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJodHRwczovL2F1dGguY29uZHVrdG9yLmlvIiwic291cmNlQXBwbGljYXRpb24iOiJhZG1pbiIsInVzZXJNYWlsIjpudWxsLCJwYXlsb2FkIjp7InZhbGlkRm9yVXNlcm5hbWUiOiI3TWJ0N0IyY25HWVdvSEFwekd3ZlM1Iiwib3JnYW5pemF0aW9uSWQiOjcwNDA0LCJ1c2VySWQiOjgxNDU2LCJmb3JFeHBpcmF0aW9uQ2hlY2siOiIxZGYzMjJkYi00ZDVlLTQ1ODctOTQ1NS00Y2ZhZTFkOWJhYjMifX0.Bd89pZACtqKn5h0_il38oYJ6s8BOtg5NRlZszvlpnpk\";");
        properties.setProperty("sasl.mechanism", "PLAIN");

        // Create consumer configs
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());

        properties.setProperty("group.id", groupId);


        // None: if we don't have any existing consumer groups, then we failed (i.e we must have a consumer group)
        // earliest: read from beginning of topic
        // latest: read only new messages
        properties.setProperty("auto.offset.reset", "earliest");

        // Create a consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        // subscribe to a topic
        consumer.subscribe(Arrays.asList(topic));

        // poll for new data
        while (true) {
            log.info("Polling...");

            // poll for new data
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

            for (ConsumerRecord<String, String> record : records) {
                log.info("Key: {}, Value: {}, Partition: {}, Offset: {}", record.key(), record.value(), record.partition(), record.offset());
            }
        }
    }
}
