package com.distributed.system.services;

import io.restassured.response.Response;
import static io.restassured.RestAssured.given;
import com.distributed.system.models.User;

public class UserService {
    private String baseUrl;

    public UserService(String baseUrl) {
        this.baseUrl = baseUrl;
    }

    public UserService() {
    }

    public Response createUser(User user) {
        return given()
                .contentType("application/json")
                .baseUri(this.baseUrl != null ? this.baseUrl : "http://localhost:8080")
                .body(user)
                .when()
                .post("/users");
    }
}