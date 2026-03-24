package com.distributed.system.tests.api;

import com.distributed.system.DistributedSystemApplication;
import com.distributed.system.tests.BaseIntegrationTest;
import io.restassured.RestAssured;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.is;

@SpringBootTest(classes = DistributedSystemApplication.class, webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
public class UserApiContainerTest extends BaseIntegrationTest {

    // We inject the port on which Spring started Tomcat in the test
    @LocalServerPort
    private Integer port;

    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:15-alpine");

    static void configureProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);
    }

    @BeforeEach
    void setUp() {
// CRITICAL: We tell RestAssured to use Spring's port, not 8080
        RestAssured.baseURI = "http://localhost";
        RestAssured.port = port;
    }

    @Test
    void shouldCreateUserTestContainer() {
        String userJson = "{\"name\": \"TestContainerUser\", \"email\": \"tc@example.com\"}";

        given()
                .contentType("application/json")
                .body(userJson)
                .when()
                .post("/api/users")
                .then()
                .statusCode(201)
                .body("name", is("TestContainerUser"));
    }
}