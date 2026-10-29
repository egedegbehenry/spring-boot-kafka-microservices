package com.henry.producerservice.service;

import com.henry.producerservice.event.OrderCreatedEvent;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class OrderProducerServiceTests {
    @Mock KafkaTemplate<String, OrderCreatedEvent> template;
    private final OrderCreatedEvent event = new OrderCreatedEvent("ORD-9010", "CUST-101", "Laptop", 2, "CREATED");

    @Test
    void sendsToV2UsingOrderIdAsKeyAndWaitsForAcknowledgment() {
        var acknowledgment = new CompletableFuture<SendResult<String, OrderCreatedEvent>>();
        when(template.send("orders.created.v2", event.orderId(), event)).thenReturn(acknowledgment);
        var result = new OrderProducerService(template).sendOrder(event);
        assertFalse(result.isDone());
        var metadata = new org.apache.kafka.clients.producer.RecordMetadata(
                new org.apache.kafka.common.TopicPartition("orders.created.v2", 0), 7, 0, 0, 0, 0);
        var sent = new SendResult<>(new org.apache.kafka.clients.producer.ProducerRecord<>(
                "orders.created.v2", event.orderId(), event), metadata);
        acknowledgment.complete(sent);
        assertSame(sent, result.join());
        verify(template).send("orders.created.v2", event.orderId(), event);
    }

    @Test
    void asynchronousFailureIsTranslated() {
        when(template.send("orders.created.v2", event.orderId(), event))
                .thenReturn(CompletableFuture.failedFuture(new IllegalStateException("broker unavailable")));
        var result = new OrderProducerService(template).sendOrder(event);
        assertInstanceOf(OrderPublicationException.class, assertThrows(CompletionException.class, result::join).getCause());
    }

    @Test
    void synchronousFailureIsTranslatedIntoFailedFuture() {
        when(template.send("orders.created.v2", event.orderId(), event)).thenThrow(new IllegalStateException("metadata timeout"));
        var result = new OrderProducerService(template).sendOrder(event);
        assertInstanceOf(OrderPublicationException.class, assertThrows(CompletionException.class, result::join).getCause());
    }
}
