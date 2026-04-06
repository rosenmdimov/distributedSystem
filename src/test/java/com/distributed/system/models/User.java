package com.distributed.system.models;

import org.apache.kafka.common.protocol.types.Field;

public class User {
    public String name;
    public String email;

    public User(String name, String email) {
        this.name = name;
        this.email = email;
    }
}