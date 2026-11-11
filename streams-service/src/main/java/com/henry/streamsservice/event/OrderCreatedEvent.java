package com.henry.streamsservice.event;

public record OrderCreatedEvent(
        String orderId,
        String customerId,
        String product,
        int quantity,
        String status
) {
}