package com.henry.consumer.event;

// Local representation of the JSON contract; no dependency on the producer's Java package is needed.
public record OrderCreatedEvent(
        String orderId,
        String customerId,
        String product,
        int quantity,
        String status
) {
}
