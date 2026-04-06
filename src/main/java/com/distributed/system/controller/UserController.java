package com.distributed.system.controller;

import com.distributed.system.model.User;
import com.distributed.system.repository.UserRepository;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.*;
import org.springframework.http.HttpStatus;

import java.util.List;

@RestController
@RequestMapping("/api/users")
public class UserController {

    private final UserRepository repository;
    private final KafkaTemplate<String, String> kafkaTemplate;

    public UserController(UserRepository repository, KafkaTemplate<String, String> kafkaTemplate) {
        this.repository = repository;
        this.kafkaTemplate = kafkaTemplate;
    }

    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    @Transactional
    public User create(@RequestBody User user) {
        User savedUser = repository.save(user);
        // Send a message to Kafka after a successful write to the DB
        kafkaTemplate.send("user-topic", String.valueOf(savedUser.getId()), "Created user: " + savedUser.getEmail());
        return savedUser;
    }

    @GetMapping
    public List<User> getAll() {
        return repository.findAll();
    }

    @GetMapping("/{id}")
    public User getById(@PathVariable Long id) {
        return repository.findById(id).orElseThrow();
    }
}