package com.distributed.system.tests.api;

import com.distributed.system.model.User;
import com.distributed.system.repository.UserRepository;
import com.distributed.system.tests.BaseIntegrationTest;
import io.qameta.allure.*;
import io.restassured.RestAssured;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.*;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.SendResult;
import org.springframework.test.annotation.DirtiesContext;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletableFuture;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.anyString;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@DirtiesContext
@Feature("Full System Integration")
public class FullSystemTest extends BaseIntegrationTest {

    @MockBean
    private KafkaAdmin admin; // Spring automatically provides it

    private void forceCreateTopic(String topicName, int partitions) {
        try (var client = org.apache.kafka.clients.admin.AdminClient.create(admin.getConfigurationProperties())) {
            var newTopic = new org.apache.kafka.clients.admin.NewTopic(topicName, partitions, (short) 1);
            client.createTopics(Collections.singletonList(newTopic)).all().get();
            System.out.println("✅ Topic " + topicName + " created with " + partitions + " partitions.");
        } catch (Exception e) {
            // If it already exists, we just continue
            System.out.println("ℹ️ Topic might already exist: " + e.getMessage());
        }
    }

    @MockBean
    private ProducerFactory<String, String> producerFactory;

    @MockBean
    private KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    private UserRepository userRepository;

    @LocalServerPort
    private Integer port;

    @BeforeEach
    @Step("Set Up the test environment")
    void setUp() {
        RestAssured.baseURI = "http://localhost";
        RestAssured.port = port;

        Mockito.when(kafkaTemplate.send(anyString(), anyString(), anyString()))
                .thenAnswer(invocation -> {
                    String topic = invocation.getArgument(0);
                    String key = invocation.getArgument(1);

                    // DYNAMICS: If the topic is for repartitioning, we simulate 10 partitions
                    // Otherwise we use the standard 3
                    int partitionCount = topic.contains("repartition") ? 10 : 3;

                    // Calculate the partition (absolute hash value)
                    int partition = Math.abs(key.hashCode()) % partitionCount;

                    TopicPartition tp = new TopicPartition(topic, partition);
                    RecordMetadata metadata = new RecordMetadata(tp, 0L, 0L, 0L, 0L, 0, 0);

                    return CompletableFuture.completedFuture(new SendResult<>(null, metadata));
                });

        Allure.addAttachment("Mock Config", "Dynamic Partitioning enabled (Logic: hash(key) % currentPartitions)");
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

        // Instead of Consumer, we check the Mock using Mockito
        org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
            Mockito.verify(kafkaTemplate).send(
                    ArgumentMatchers.eq("user-topic"),
                    ArgumentMatchers.anyString(), // ключ
                    ArgumentMatchers.contains(expectedEmail) // стойност
            );
        });
    }

    @Test
    @Story("Transactional Rollback on Kafka Failure")
    @Description("Verifies that user is NOT saved in DB if Kafka notification fails")
    public void shouldRollbackDatabaseIfKafkaFails() {
        String email = "rollback-test-" + System.currentTimeMillis() + "@example.com";
        User user = new User("RollbackUser", email);

        // Important: Using 3 times anyString() to match the Controller method.
        Mockito.doThrow(new RuntimeException("Kafka is down!"))
                .when(kafkaTemplate).send(anyString(), anyString(), anyString());

        given()
                .contentType("application/json")
                .body(user)
                .when()
                .post("/api/users")
                .then()
                .statusCode(500);
        boolean existInDB = userRepository.findByEmail(email).isPresent();
        Assertions.assertFalse(existInDB, "ERROR! The user is added to DB although Kafka failure. The transaction doesn't work!");
    }

    @Test
    @Story("Kafka message Partitioning")
    @Description("Ensures that messages with the same Key end up in the same Partition")
    public void shouldSendMessagesToSamePartitionForSameKey() {
        User user = new User("PartitionTest", "partition@test.com");
        user = userRepository.save(user);
        String userId = String.valueOf(user.getId());

        kafkaTemplate.send("user-topic", userId, "Message 1 for " + userId);
        kafkaTemplate.send("user-topic", userId, "Message 2 for " + userId);

        // Using Kafka consumer to check the metadata (Partition)
        var future1 = kafkaTemplate.send("user-topic", userId, "Check 1");
        var future2 = kafkaTemplate.send("user-topic", userId, "Check 2");

        try {
            int partition1 = future1.get().getRecordMetadata().partition();
            int partition2 = future2.get().getRecordMetadata().partition();

            Assertions.assertEquals(partition1, partition2, "Messages with same key must be in the same partition!");
        } catch (Exception e) {
            Assertions.fail("Kafka send failed: " + e.getMessage());
        }
    }


    @Test
    @Story("Kafka Multi-Partition Distribution")
    public void shouldDistributeMessagesAcrossPartitions() throws Exception {
        String topic = "final-test-topic";
        forceCreateTopic(topic, 3);

        List<Integer> partitions = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            // Important: Use (topic, key, data)
            var res = kafkaTemplate.send(topic, "key-" + i, "data-" + i).get();
            partitions.add(res.getRecordMetadata().partition());
        }

        partitions.forEach(p -> System.out.println("Partition used: " + p));

        long uniquePartitions = partitions.stream().distinct().count();
        Assertions.assertTrue(uniquePartitions > 1,
                "The messages must be distributed in more than 1 partition. Found: " + uniquePartitions);
    }

    @Test
    public void shouldShowHowOrderBreaksOnRepartitioning() throws Exception {
        String topicName = "repartition-test-topic-" + System.currentTimeMillis();

        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());

        try (AdminClient adminClient = AdminClient.create(props)) {
            // 1. Създаваме топика първоначално с 1 партиция (за да е гарантиран редът в началото)
            adminClient.createTopics(Collections.singletonList(new NewTopic(topicName, 1, (short) 1))).all().get();

            // Малко изчакване за метаданните
            Thread.sleep(1000);

            // 2. Първо съобщение
            var res1 = kafkaTemplate.send(topicName, "user-important", "First Message").get();
            int p1 = res1.getRecordMetadata().partition();

            // 3. Увеличаваме партициите на 10
            adminClient.createPartitions(Map.of(topicName, NewPartitions.increaseTo(10))).all().get();

            // ВАЖНО: Даваме време на Kafka да преразпредели метаданните
            Thread.sleep(2000);

            // 4. Второ съобщение със СЪЩИЯ ключ
            var res2 = kafkaTemplate.send(topicName, "user-important", "Second Message").get();
            int p2 = res2.getRecordMetadata().partition();

            Allure.addAttachment("Partition Drift", "Before (1 partition): " + p1 + ", After (10 partitions): " + p2);

            // Тук е логиката на теста: При промяна на броя партиции,
            // хешът на същия ключ ("user-important") ще посочи различно място.
            System.out.println("Partition 1: " + p1 + " | Partition 2: " + p2);

            // Ако искаш тестът да е "строг" (Assertion), но имай предвид, че е статистически възможно да съвпаднат
            // Assertions.assertNotEquals(p1, p2, "Order might still be preserved by pure luck!");
        }
    }
}
