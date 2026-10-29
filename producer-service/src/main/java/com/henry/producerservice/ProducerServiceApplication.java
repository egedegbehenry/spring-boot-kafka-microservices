package com.henry.producerservice;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

// Discovers producer components and enables Spring Boot auto-configuration.
@SpringBootApplication
public class ProducerServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(ProducerServiceApplication.class, args);
    }

}
