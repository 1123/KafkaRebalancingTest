package io.confluent.examples.rebalancing.longrunning;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.UUID;

import static io.confluent.examples.rebalancing.longrunning.RebalancingWithOnGoingConsumptionTest.BOOTSTRAP_SERVERS;
import static io.confluent.examples.rebalancing.longrunning.RebalancingWithOnGoingConsumptionTest.PROCESSING_TIME;

@Slf4j
public class ConsumerThread implements Runnable {

    private static final String GROUP_ID = UUID.randomUUID().toString();

    @Override
    public void run() {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", BOOTSTRAP_SERVERS);
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("group.id", GROUP_ID);
        properties.put("max.poll.interval.ms", 5 * 60 * 1000);
        properties.put("max.poll.records", 1);
        properties.put("auto.offset.reset", "earliest");

        try(KafkaConsumer<String,String> kafkaConsumer = new KafkaConsumer<>(properties)) {
            kafkaConsumer.subscribe(Collections.singletonList(RebalancingWithOnGoingConsumptionTest.TOPIC_NAME));
            while (true) {
                var records = kafkaConsumer.poll(Duration.ofSeconds(100));
                log.info("Received poll result with {} records", records.count());
                log.info(kafkaConsumer.assignment().toString());
                records.forEach(this::processRecord);
                log.info("Calling Commit Sync");
                kafkaConsumer.commitSync();
                log.info("Messages successfully committed");
            }
        }
    }

    private void processRecord(ConsumerRecord<String, String> r) {
        log.info("Start processing record: {}", r);
        try { Thread.sleep(PROCESSING_TIME); } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        log.info("Finished processing records: {}", r);
    }
}
