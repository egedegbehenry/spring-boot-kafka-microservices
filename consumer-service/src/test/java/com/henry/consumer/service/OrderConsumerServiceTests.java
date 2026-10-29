package com.henry.consumer.service;

import com.henry.consumer.event.OrderCreatedEvent;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;
import static org.junit.jupiter.api.Assertions.*;

@ExtendWith(OutputCaptureExtension.class)
class OrderConsumerServiceTests {
    private final OrderConsumerService consumer = new OrderConsumerService();

    @Test
    void processesAndLogsEveryField(CapturedOutput output) {
        consumer.consumeOrder(new OrderCreatedEvent("ORD-9010", "CUST-101", "Laptop", 2, "CREATED"));
        assertTrue(output.getOut().contains("orderId=ORD-9010 customerId=CUST-101 product=Laptop quantity=2 status=CREATED"));
    }

    @Test
    void rejectsInvalidEvents() {
        assertThrows(IllegalArgumentException.class, () -> consumer.consumeOrder(null));
        assertThrows(IllegalArgumentException.class, () -> consumer.consumeOrder(
                new OrderCreatedEvent("", "CUST-101", "Laptop", 2, "CREATED")));
        assertThrows(IllegalArgumentException.class, () -> consumer.consumeOrder(
                new OrderCreatedEvent("ORD-9010", null, "Laptop", 2, "CREATED")));
        assertThrows(IllegalArgumentException.class, () -> consumer.consumeOrder(
                new OrderCreatedEvent("ORD-9010", "CUST-101", " ", 2, "CREATED")));
        assertThrows(IllegalArgumentException.class, () -> consumer.consumeOrder(
                new OrderCreatedEvent("ORD-9010", "CUST-101", "Laptop", 0, "CREATED")));
        assertThrows(IllegalArgumentException.class, () -> consumer.consumeOrder(
                new OrderCreatedEvent("ORD-9010", "CUST-101", "Laptop", 2, "PAID")));
    }
}
