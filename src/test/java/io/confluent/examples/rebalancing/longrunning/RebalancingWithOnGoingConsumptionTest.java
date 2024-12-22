package io.confluent.examples.rebalancing.longrunning;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;

import java.util.*;
import java.util.concurrent.ExecutionException;

@Slf4j
public class RebalancingWithOnGoingConsumptionTest {

    public static final String BOOTSTRAP_SERVERS = "localhost:65437";
    public static final String TOPIC_NAME = "rebalancing-test";
    private static final int NUM_CONSUMERS = 6;
    private static final int NUM_MESSAGES = 100;
    static final long PROCESSING_TIME = 120 * 1000;

    @Test
    public void testRebalancingWithOngoingConsumption() throws InterruptedException, ExecutionException {
        this.createTopic();
        log.info("Producing {} messages to topic {}", NUM_MESSAGES, TOPIC_NAME);
        produceMessages();
        log.info("Starting {} consumer threads. Each message processing will take {} milliseconds to process." ,
                NUM_CONSUMERS, PROCESSING_TIME
        );
        var consumers = this.startConsumerThreads();
        Thread.sleep(20 * 1000);
        log.info("Stopping one consumer. This will trigger a rebalance. ");
        consumers.get(1).interrupt();
        consumers.forEach(c -> {
            try {
                c.join();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });

    }

    private List<Thread> startConsumerThreads( ) {
        List<Thread> consumerThreads= new ArrayList<>();
        for (int i = 0; i < NUM_CONSUMERS; i++) {
            Thread t = new Thread(new ConsumerThread());
            t.start();
            consumerThreads.add(t);
        }
        log.info("Started consumer threads");
        return consumerThreads;
    }

    private void createTopic() throws ExecutionException, InterruptedException {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", BOOTSTRAP_SERVERS);
        AdminClient adminClient = AdminClient.create(properties);
        try {
            log.info("Trying to delete topic");
            adminClient.deleteTopics(Collections.singletonList(TOPIC_NAME)).all().get();
            Thread.sleep(1000);
        } catch (ExecutionException e) {
            log.info("Topic {} did not exist. Therefore nothing was deleted. ", TOPIC_NAME);
        }
        adminClient.createTopics(
                Collections.singletonList(new NewTopic(TOPIC_NAME, NUM_CONSUMERS, (short) 1))
        ).all().get();
        log.info("Successfully created topic {}", TOPIC_NAME);
        adminClient.close();
    }

    private void produceMessages() {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", BOOTSTRAP_SERVERS);
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(properties)) {
            for (int i = 0; i < NUM_MESSAGES; i++) {
                producer.send(
                        new ProducerRecord<>(
                                RebalancingWithOnGoingConsumptionTest.TOPIC_NAME,
                                UUID.randomUUID().toString(),
                                "bar"
                        )
                ).get();
            }
            log.info("Successfully produced messages");
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

}

