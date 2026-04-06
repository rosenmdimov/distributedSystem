package com.distributed.system.tests.integration;

import io.qameta.allure.Allure;
import io.qameta.allure.Feature;
import io.qameta.allure.Step;
import io.qameta.allure.Story;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.shaded.org.awaitility.Awaitility;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

public class KafkaIntegrationTest {

    static KafkaContainer kafkaContainer;

    @BeforeAll
    public static void startKafka() {
        kafkaContainer = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.1"));
        kafkaContainer.start();
    }

    @AfterAll
    @Step("Clean up the test environment")
    public static void stopKafka() {
        if (kafkaContainer != null) kafkaContainer.stop();
    }

    @Test
    @Feature("Kafka Messaging")
    @Story("Produce and Consume")
    public void shouldProduceAndConsumeMessage() {
        String topic = "test-topic";
        String message = "Hello Kafka";

        sendMessageToKafka(topic, message);
        verifyMessageReceived(topic, message);
    }

    @Step("Sending message '{message}' to topic '{topic}'")
    private void sendMessageToKafka(String topic, String message) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            producer.send(new ProducerRecord<>(topic, "key1", message));
            producer.flush();
        }
    }

    @Step("Waiting and verifying that message '{expectedMessage}' is received from '{topic}'")
    private void verifyMessageReceived(String topic, String expectedMessage) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group-" + System.currentTimeMillis());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(topic));

        Awaitility.await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
            assertFalse(records.isEmpty(), "No messages found!");

            ConsumerRecord<String, String> record = records.iterator().next();
            Allure.addAttachment("Found Value", record.value());
            assertEquals(expectedMessage, record.value());
        });
        consumer.close();
    }
//        ConsumerRecord<String, String> record =
//                consumer.poll(Duration.ofSeconds(5)).iterator().next();
//
//        assertEquals("Hello Kafka", record.value());
//        consumer.close();
    }
