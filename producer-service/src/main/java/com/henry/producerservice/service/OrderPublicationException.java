package com.henry.producerservice.service;

public class OrderPublicationException extends RuntimeException {
    public OrderPublicationException(Throwable cause) {
        super("Unable to publish order to Kafka", cause);
    }
}
