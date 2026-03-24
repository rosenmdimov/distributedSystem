package com.distributed.system.tests.api;

import com.distributed.system.model.User;
import com.distributed.system.tests.BaseIntegrationTest;
import io.qameta.allure.*;
import io.restassured.RestAssured;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.*;
import org.springframework.boot.test.web.server.LocalServerPort;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.is;

@Feature("Full System Integration")
public class FullSystemTest extends BaseIntegrationTest {

    @LocalServerPort
    private Integer port;

    @BeforeEach
    void setUp() {
        RestAssured.port = port;
    }

    @Test
    @Story("API to Database and Kafka Flow")
    @Description("Creates a user via API and verifies DB record and Kafka notification")
    public void shouldProcessUserCreationFully() {
        String email = "rossen@example.com";
        User user = new User();
        user.setName("Rossen");
        user.setEmail(email);

        stepCreateUser(user);
        stepVerifyKafkaNotification(email);
    }

    @Step("Create user via REST API")
    private void stepCreateUser(User user) {
        given()
                .contentType("application/json")
                .body(user)
                .when()
                .post("/api/users")
                .then()
                .statusCode(201)
                .body("email", is(user.getEmail()));
    }

    @Step("Verify Kafka notification for user {expectedEmail}")
    private void stepVerifyKafkaNotification(String expectedEmail) {
        Properties props = new Properties();
        // We use the 'kafka' object from BaseIntegrationTest
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group-" + System.currentTimeMillis());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList("user-topic"));

        org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> {
            var records = consumer.poll(Duration.ofMillis(500));
            Assertions.assertFalse(records.isEmpty(), "Kafka message was not sent!");
            String lastMessage = records.iterator().next().value();
            Allure.addAttachment("Kafka Notification", lastMessage);
            Assertions.assertTrue(lastMessage.contains(expectedEmail));
        });
        consumer.close();
    }
}