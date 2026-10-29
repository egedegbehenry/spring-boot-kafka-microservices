package com.henry.producerservice.controller;

import com.henry.producerservice.event.OrderCreatedEvent;
import com.henry.producerservice.service.OrderProducerService;
import jakarta.validation.Valid;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import java.util.concurrent.CompletableFuture;

//Tells Spring that this class handles HTTP requests.
@RestController
//defines the URL
@RequestMapping("/api/orders")
//Responsible for receiving the request
public class OrderController {
    private final OrderProducerService producerService;

    public OrderController(OrderProducerService producerService) {
        this.producerService = producerService;
    }

    // @Valid checks the event's field constraints before this method publishes anything.
    @PostMapping(consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
    public CompletableFuture<ResponseEntity<PublicationResponse>> createOrder(
            @Valid @RequestBody OrderCreatedEvent event) {
        // Return 202 after Kafka acknowledges the send; consumer processing happens independently.
        return producerService.sendOrder(event).thenApply(result -> ResponseEntity.accepted().body(
                new PublicationResponse(event.orderId(), "PUBLISHED", result.getRecordMetadata().topic(),
                        result.getRecordMetadata().partition(), result.getRecordMetadata().offset())));
    }

    public record PublicationResponse(String orderId, String status, String topic, int partition, long offset) { }
}
