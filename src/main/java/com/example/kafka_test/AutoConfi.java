package com.example.kafka_test;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;



@Configuration
public class AutoConfi {
    @Value("${spring.kafka.topic}")
    public String topicName;

@Bean
    public NewTopic libraryEventsTopic() {
    return TopicBuilder.name(topicName).replicas(3).partitions(3).build();
}
}
