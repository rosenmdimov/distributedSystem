package com.distributed.system.tests.api;

import com.distributed.system.DistributedSystemApplication;
import com.distributed.system.tests.BaseIntegrationTest;
import io.restassured.RestAssured;
import io.restassured.http.ContentType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.*;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles("test")
public class UserApiSpringBootTest extends BaseIntegrationTest {

    @LocalServerPort
    private Integer port;

    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:15-alpine")
            .withDatabaseName("testdb")
            .withUsername("testuser")
            .withPassword("testpass");

    static void configureProperties(DynamicPropertyRegistry registry) {
// These lines tell Spring exactly where the Docker container is
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);
// Important: we are telling Hibernate to use Postgres dialect
        registry.add("spring.jpa.database-platform", () -> "org.hibernate.dialect.PostgreSQLDialect");

    }

    @BeforeEach
    void setUp() {
        RestAssured.baseURI = "http://localhost";
        RestAssured.port = port;
    }

    @Test
    void shouldCreateAndRetrieveUser() {
        String userJson = "{\"name\": \"Rossen\", \"email\": \"rossen@example.com\"}";

// 1. Create a user through the API
        given()
                .contentType(ContentType.JSON)
                .body(userJson)
                .when()
                .post("/api/users")
                .then()
                .statusCode(201) // Check for "Created"
                .body("name", is("Rossen"))
                .body("id", notNullValue());

// 2. We check if the user list already contains it
        given()
                .when()
                .get("/api/users")
                .then()
                .statusCode(200)
                .body("size()", greaterThanOrEqualTo(1))
                .body("name", hasItem("Rossen"));
    }
}