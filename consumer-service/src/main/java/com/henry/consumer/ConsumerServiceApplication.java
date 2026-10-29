package com.henry.consumer;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

// Enables auto-configuration and discovers components in com.henry.consumer and its subpackages.

@SpringBootApplication
public class ConsumerServiceApplication {
    public static void main(String[] args) {
        // Starts the consumer application, including its Kafka listener and Actuator HTTP server.
        SpringApplication.run(ConsumerServiceApplication.class, args);
    }
}
