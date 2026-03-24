package com.distributed.system.base;

import io.qameta.allure.restassured.AllureRestAssured;
import io.restassured.RestAssured;
import org.junit.jupiter.api.BeforeAll;

public class BaseTest extends TestContainersSetup {

    @BeforeAll
    static void globalSetup() {
        // 1. Configure the Allure filter once for the entire project
        RestAssured.filters(new AllureRestAssured());

        // 2. Configure the dynamic address from the container
        RestAssured.baseURI = "http://" + mockServer.getHost();
        RestAssured.port = mockServer.getMappedPort(1080);

        System.out.println("🚀 Allure listener and RestAssured initialized!");
    }
}