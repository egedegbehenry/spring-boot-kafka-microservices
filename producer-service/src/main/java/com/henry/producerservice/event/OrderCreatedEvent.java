package com.henry.producerservice.event;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Positive;

// Immutable JSON event; validation annotations define the producer's accepted request values.
public record OrderCreatedEvent(
        @NotBlank String orderId,
        @NotBlank String customerId,
        @NotBlank String product,
        // Integer preserves a missing value as null so validation can report it explicitly.
        @NotNull @Positive Integer quantity,
        @NotBlank @Pattern(regexp = "CREATED", message = "must be CREATED") String status
) { }
