package com.distributed.system.tests;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.config.TopicBuilder;

//@TestConfiguration
public class TestKafkaConfig {

    @Bean
    public NewTopic userTopic() {
        // Topic name, partitions count (e.g. 3), replications (1 for test)
        return TopicBuilder.name("user-topic")
                .partitions(3)
                .replicas(1)
                .build();
    }
}
