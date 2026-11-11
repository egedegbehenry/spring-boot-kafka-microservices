package com.henry.streamsservice.event;

public record ProcessedOrderEvent(
        String orderId,
        String product,
        int quantity,
        String processingStatus
) {
}