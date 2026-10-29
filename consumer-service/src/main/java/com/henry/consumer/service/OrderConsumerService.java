package com.henry.consumer.service;

import com.henry.consumer.event.OrderCreatedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class OrderConsumerService {
    private static final Logger log = LoggerFactory.getLogger(OrderConsumerService.class);

    // Spring passes deserialized events here and uses the configured group to track offsets.
    @KafkaListener(topics = "orders.created.v2", groupId = "${spring.kafka.consumer.group-id}")
    public void consumeOrder(OrderCreatedEvent event) {
        // Check business values too: valid JSON can still contain an invalid order.
        if (event == null || isBlank(event.orderId()) || isBlank(event.customerId())
                || isBlank(event.product()) || event.quantity() <= 0 || !"CREATED".equals(event.status())) {
            throw new IllegalArgumentException("Invalid order-created event");
        }
        log.info("Order notification processed successfully: orderId={} customerId={} product={} quantity={} status={}",
                event.orderId(), event.customerId(), event.product(), event.quantity(), event.status());
    }

    private static boolean isBlank(String value) {
        return value == null || value.isBlank();
    }
}
