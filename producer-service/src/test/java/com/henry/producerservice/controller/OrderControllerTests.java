package com.henry.producerservice.controller;

import com.henry.producerservice.event.OrderCreatedEvent;
import com.henry.producerservice.service.OrderProducerService;
import com.henry.producerservice.service.OrderPublicationException;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.support.SendResult;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import java.util.concurrent.CompletableFuture;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@WebMvcTest(OrderController.class)
@Import(GlobalExceptionHandler.class)
class OrderControllerTests {
    private static final String VALID = """
            {"orderId":"ORD-9010","customerId":"CUST-101","product":"Laptop","quantity":2,"status":"CREATED"}
            """;
    @Autowired MockMvc mvc;
    @MockitoBean OrderProducerService producer;

    @Test
    void validOrderReturnsAcknowledgedMetadata() throws Exception {
        var event = new OrderCreatedEvent("ORD-9010", "CUST-101", "Laptop", 2, "CREATED");
        var metadata = new RecordMetadata(new TopicPartition("orders.created.v2", 0), 7, 0, 0, 0, 0);
        when(producer.sendOrder(any())).thenReturn(CompletableFuture.completedFuture(
                new SendResult<>(new ProducerRecord<>("orders.created.v2", event.orderId(), event), metadata)));
        MvcResult pending = mvc.perform(post("/api/orders").contentType("application/json").content(VALID))
                .andExpect(request().asyncStarted()).andReturn();
        mvc.perform(asyncDispatch(pending)).andExpect(status().isAccepted())
                .andExpect(jsonPath("$.orderId").value("ORD-9010"))
                .andExpect(jsonPath("$.status").value("PUBLISHED"))
                .andExpect(jsonPath("$.topic").value("orders.created.v2"))
                .andExpect(jsonPath("$.offset").value(7));
        verify(producer).sendOrder(event);
    }

    @Test
    void responseWaitsForKafkaAcknowledgment() throws Exception {
        var future = new CompletableFuture<SendResult<String, OrderCreatedEvent>>();
        when(producer.sendOrder(any())).thenReturn(future);
        var pending = mvc.perform(post("/api/orders").contentType("application/json").content(VALID))
                .andExpect(request().asyncStarted()).andReturn();
        org.junit.jupiter.api.Assertions.assertFalse(future.isDone());
        future.completeExceptionally(new OrderPublicationException(new RuntimeException("broker unavailable")));
        mvc.perform(asyncDispatch(pending)).andExpect(status().isServiceUnavailable());
    }

    @Test
    void invalidFieldsReturnUsefulErrorsWithoutPublishing() throws Exception {
        mvc.perform(post("/api/orders").contentType("application/json").content("""
                {"orderId":" ","customerId":"","product":"","quantity":0,"status":"PAID"}
                """)).andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.errors.orderId").exists())
                .andExpect(jsonPath("$.errors.customerId").exists())
                .andExpect(jsonPath("$.errors.product").exists())
                .andExpect(jsonPath("$.errors.quantity").exists())
                .andExpect(jsonPath("$.errors.status").exists());
        verifyNoInteractions(producer);
    }

    @Test
    void missingFieldsAreRejected() throws Exception {
        mvc.perform(post("/api/orders").contentType("application/json").content("{}"))
                .andExpect(status().isBadRequest()).andExpect(jsonPath("$.errors.quantity").exists());
        verifyNoInteractions(producer);
    }

    @Test
    void malformedJsonIsBadRequest() throws Exception {
        mvc.perform(post("/api/orders").contentType("application/json").content("{"))
                .andExpect(status().isBadRequest()).andExpect(jsonPath("$.status").value(400));
        verifyNoInteractions(producer);
    }

    @Test
    void publicationFailureReturns503WithoutLeakingDetails() throws Exception {
        when(producer.sendOrder(any())).thenReturn(CompletableFuture.failedFuture(
                new OrderPublicationException(new RuntimeException("private broker details"))));
        var pending = mvc.perform(post("/api/orders").contentType("application/json").content(VALID))
                .andReturn();
        mvc.perform(asyncDispatch(pending)).andExpect(status().isServiceUnavailable())
                .andExpect(jsonPath("$.detail").value(
                        "Kafka could not confirm publication. An order may have been published; check before retrying."));
    }

    @Test
    void unexpectedFailureReturns500() throws Exception {
        when(producer.sendOrder(any())).thenThrow(new IllegalStateException("internal details"));
        mvc.perform(post("/api/orders").contentType("application/json").content(VALID))
                .andExpect(status().isInternalServerError())
                .andExpect(jsonPath("$.detail").value("An unexpected error occurred"));
    }

    @Test
    void unsupportedMediaTypeIs415() throws Exception {
        mvc.perform(post("/api/orders").contentType("text/plain").content("order"))
                .andExpect(status().isUnsupportedMediaType());
        verifyNoInteractions(producer);
    }
}
