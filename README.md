# Distributed System Integration Testing Suite

This project demonstrates a robust integration testing architecture for a Spring Boot application using **Testcontainers**, **PostgreSQL**, **Kafka**, and **Allure Reporting**.

## 🚀 Features

- **Spring Boot 3.2.1** backend with JPA/Hibernate.
- **PostgreSQL Integration**: Real database testing using Docker containers.
- **Kafka Integration**: End-to-end messaging verification with `KafkaContainer`.
- **Automated Reporting**: Detailed test execution reports with Allure.
- **REST Assured**: Fluent API testing for the controller layer.

## 🛠 Prerequisites

- Java 17 or higher
- Docker Desktop (Required for Testcontainers)
- Maven 3.9+

## 🔧 Installation & Running Tests

1. **Clone the repository:**
   ```bash
   git clone [https://github.com/your-username/distributed-system.git](https://github.com/your-username/distributed-system.git)
   cd distributed-system

## 🚀 Run all integration tests:
      mvn clean test
Note: Testcontainers will automatically pull and start PostgreSQL and Kafka images.

## Generate and open the Allure Report:
      mvn allure:serve

## 🏗 Project Structure
   * src/main/java: Spring Boot application logic (Controllers, Repositories, Entities).

   * src/test/java:

      * api/: REST API integration tests (Container-based).

      * integration/: Infrastructure tests (Kafka, Database connectivity).

   * src/test/resources: Test-specific configurations and AOP settings.

## 📊 Testing Stack
   - Technology        | Purpose
   - Testcontainers    | Isolated Docker environments for DB and Messaging
   - RestAssuredAPI    | validation and status code assertions
   - Awaitility        | Handling asynchronous Kafka message polling
   - AllureVisual      | storytelling of test results

Created by Rossen Dimov - 2026

   