package com.henry.producerservice.service;

import com.henry.producerservice.event.OrderCreatedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import java.util.concurrent.CompletableFuture;

@Service
public class OrderProducerService {
    private static final String TOPIC = "orders.created.v2";
    private static final Logger log = LoggerFactory.getLogger(OrderProducerService.class);
    private final KafkaTemplate<String, OrderCreatedEvent> kafkaTemplate;

    public OrderProducerService(KafkaTemplate<String, OrderCreatedEvent> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public CompletableFuture<SendResult<String, OrderCreatedEvent>> sendOrder(OrderCreatedEvent event) {
        try {
            // The order ID is the partitioning key; the future completes when Kafka confirms the send.
            return kafkaTemplate.send(TOPIC, event.orderId(), event).handle((result, failure) -> {
                if (failure != null) {
                    throw publicationFailure(event, failure);
                }
                var metadata = result.getRecordMetadata();
                log.info("Kafka acknowledged orderId={} topic={} partition={} offset={}",
                        event.orderId(), metadata.topic(), metadata.partition(), metadata.offset());
                return result;
            });
        } catch (RuntimeException failure) {
            // Failures before a send future is returned use the same asynchronous error path.
            return CompletableFuture.failedFuture(publicationFailure(event, failure));
        }
    }

    private OrderPublicationException publicationFailure(OrderCreatedEvent event, Throwable failure) {
        log.error("Kafka publication failed for orderId={}", event.orderId(), failure);
        return new OrderPublicationException(failure);
    }
}
