package com.henry.consumer.config;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JacksonJsonDeserializer;
import com.henry.consumer.event.OrderCreatedEvent;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.List;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(OutputCaptureExtension.class)
class KafkaConsumerConfigurationTests {
    @Test
    void jsonMapsToConsumersOwnRecord() {
        try (var deserializer = new JacksonJsonDeserializer<OrderCreatedEvent>()) {
            deserializer.configure(Map.of(
                    JacksonJsonDeserializer.VALUE_DEFAULT_TYPE, OrderCreatedEvent.class.getName(),
                    JacksonJsonDeserializer.TRUSTED_PACKAGES, "com.henry.consumer.event",
                    JacksonJsonDeserializer.USE_TYPE_INFO_HEADERS, false), false);
            var event = deserializer.deserialize("orders.created.v2", """
                    {"orderId":"ORD-9010","customerId":"CUST-101","product":"Laptop","quantity":2,"status":"CREATED"}
                    """.getBytes(StandardCharsets.UTF_8));
            assertEquals(new OrderCreatedEvent("ORD-9010", "CUST-101", "Laptop", 2, "CREATED"), event);
        }
    }

    @Test
    void malformedJsonIsCapturedForErrorHandler() {
        try (var deserializer = new ErrorHandlingDeserializer<>(new JacksonJsonDeserializer<>(OrderCreatedEvent.class))) {
            var headers = new RecordHeaders();
            assertNull(deserializer.deserialize("orders.created.v2", headers, "{bad".getBytes(StandardCharsets.UTF_8)));
            assertNotNull(headers.lastHeader("springDeserializerExceptionValue"));
        }
    }

    @Test
    void transientFailureRetriesTwiceThenLogsRecovery(CapturedOutput output) {
        var handler = new KafkaConsumerConfiguration().kafkaErrorHandler();
        var kafkaConsumer = mock(org.apache.kafka.clients.consumer.Consumer.class);
        var container = mock(org.springframework.kafka.listener.MessageListenerContainer.class);
        var record = new ConsumerRecord<>("orders.created.v2", 0, 12L, "ORD-9010", "value");
        var failure = new IllegalStateException("temporary processing failure");
        assertFalse(handler.handleOne(failure, record, kafkaConsumer, container));
        assertFalse(handler.handleOne(failure, record, kafkaConsumer, container));
        assertTrue(handler.handleOne(failure, record, kafkaConsumer, container));
        assertTrue(output.getOut().contains("skipping failed record topic=orders.created.v2 partition=0 offset=12 key=ORD-9010"));
    }

    @Test
    void invalidEventSkipsRetriesAndLogsRecovery(CapturedOutput output) {
        var handler = new KafkaConsumerConfiguration().kafkaErrorHandler();
        assertTrue(handler.handleOne(new IllegalArgumentException("invalid event"),
                new ConsumerRecord<>("orders.created.v2", 0, 13L, "bad", "value"),
                mock(org.apache.kafka.clients.consumer.Consumer.class),
                mock(org.springframework.kafka.listener.MessageListenerContainer.class)));
        assertTrue(output.getOut().contains("offset=13"));
    }
}
