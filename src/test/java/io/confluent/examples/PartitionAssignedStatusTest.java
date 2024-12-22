package io.confluent.examples;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

@Slf4j
public class PartitionAssignedStatusTest {

    /**
     * This test demonstrates how to get the partition assignment status of a KafkaConsumer.
     */

    @Test
    public void testPartitionAssignment() {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("group.id", "foo");
        try(KafkaConsumer<String,String> kafkaConsumer = new KafkaConsumer<>(properties)) {
            kafkaConsumer.subscribe(Collections.singletonList("my-topic"));
            while (true) {
                Thread.sleep(1000);
                var records = kafkaConsumer.poll(Duration.ofMillis(100));
                log.info("Received {} records", records.count());
                log.info(kafkaConsumer.assignment().toString());
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
