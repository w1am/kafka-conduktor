package io.conduktor.demos.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RoundRobinPartitioner;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallback {
    private static final Logger log = LoggerFactory.getLogger(ProducerDemo.class.getSimpleName());

    public static void main(String[] args) throws InterruptedException {
        log.info("I am a Kafka Producer!");

        // Create producer properties
        Properties properties = new Properties();

        // Connect to localhost
        // properties.setProperty("bootstrap.servers", "127.0.0.1:9092");

        // connect to Conduktor Playground
        properties.setProperty("bootstrap.servers", "cluster.playground.cdkt.io:9092");
        properties.setProperty("security.protocol", "SASL_SSL");
        properties.setProperty("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"7Mbt7B2cnGYWoHApzGwfS5\" password=\"eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJodHRwczovL2F1dGguY29uZHVrdG9yLmlvIiwic291cmNlQXBwbGljYXRpb24iOiJhZG1pbiIsInVzZXJNYWlsIjpudWxsLCJwYXlsb2FkIjp7InZhbGlkRm9yVXNlcm5hbWUiOiI3TWJ0N0IyY25HWVdvSEFwekd3ZlM1Iiwib3JnYW5pemF0aW9uSWQiOjcwNDA0LCJ1c2VySWQiOjgxNDU2LCJmb3JFeHBpcmF0aW9uQ2hlY2siOiIxZGYzMjJkYi00ZDVlLTQ1ODctOTQ1NS00Y2ZhZTFkOWJhYjMifX0.Bd89pZACtqKn5h0_il38oYJ6s8BOtg5NRlZszvlpnpk\";");
        properties.setProperty("sasl.mechanism", "PLAIN");

        // Set the producer properties
        properties.setProperty("key.serializer", StringSerializer .class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

//        properties.setProperty("batch.size", "400");

//        properties.setProperty("partitioner.class", RoundRobinPartitioner.class.getName());

        // Create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        for (int j = 0; j < 10; j++) {
            for (int i = 0; i < 30; i++) {
                // Create producer record
                ProducerRecord<String, String> record =
                        new ProducerRecord<>("demo_java", "hello world " + i);

                // Send data
                producer.send(record, (metadata, exception) -> {
                    // executes every time a record is successfully sent or an exception is thrown
                    if (exception == null) {
                        // the record was successfully sent
                        log.info("Received new metadata. \n" +
                                "Topic: " + metadata.topic() + "\n" +
                                "Partition: " + metadata.partition() + "\n" +
                                "Offset: " + metadata.offset() + "\n" +
                                "Timestamp: " + metadata.timestamp());
                    } else {
                        log.error("Error while producing", exception);
                    }
                });
            }

            Thread.sleep(500);
        }


        // tell the producer to send all data and block until done --synchronous
        producer.flush();

        // flush and close the producer
        producer.close();
    }
}
