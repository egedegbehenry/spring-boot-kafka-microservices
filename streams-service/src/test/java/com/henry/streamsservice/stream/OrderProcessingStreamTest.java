package com.henry.streamsservice.stream;

import com.henry.streamsservice.event.OrderCreatedEvent;
import com.henry.streamsservice.event.ProcessedOrderEvent;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.support.serializer.JacksonJsonSerde;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

class OrderProcessingStreamTest {

    private static final String INPUT_TOPIC = "orders.created.v2";
    private static final String OUTPUT_TOPIC = "orders.processed.v1";

    //createTestDriver() builds the same topology from your OrderProcessingStream class that
    // the running application uses. The test supplies events through TestInputTopic and
    // checks the resulting messages in TestOutputTopic.

    private TopologyTestDriver createTestDriver() {
        StreamsBuilder builder = new StreamsBuilder();

        new OrderProcessingStream().processOrders(builder);

        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-unit-test");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");

        return new TopologyTestDriver(builder.build(), config);
    }

    @Test
    void validOrderShouldBeProcessed() {

        try (TopologyTestDriver driver = createTestDriver()) {

            var orderSerde =
                    new JacksonJsonSerde<>(OrderCreatedEvent.class);

            var processedSerde =
                    new JacksonJsonSerde<>(ProcessedOrderEvent.class);

            TestInputTopic<String, OrderCreatedEvent> input =
                    driver.createInputTopic(
                            INPUT_TOPIC,
                            Serdes.String().serializer(),
                            orderSerde.serializer()
                    );

            TestOutputTopic<String, ProcessedOrderEvent> output =
                    driver.createOutputTopic(
                            OUTPUT_TOPIC,
                            Serdes.String().deserializer(),
                            processedSerde.deserializer()
                    );

            input.pipeInput(
                    "ORD-TEST-100",
                    new OrderCreatedEvent(
                            "ORD-TEST-100",
                            "CUST-100",
                            "Laptop",
                            2,
                            "CREATED"
                    )
            );

            assertFalse(output.isEmpty());

            var result = output.readKeyValue();

            assertEquals("ORD-TEST-100", result.key);
            assertEquals("ORD-TEST-100", result.value.orderId());
            assertEquals("Laptop", result.value.product());
            assertEquals(2, result.value.quantity());
            assertEquals("PROCESSED", result.value.processingStatus());

            assertTrue(output.isEmpty());
        }
    }

    @Test
    void invalidOrdersShouldBeFilteredOut() {

        try (TopologyTestDriver driver = createTestDriver()) {

            var orderSerde =
                    new JacksonJsonSerde<>(OrderCreatedEvent.class);

            var processedSerde =
                    new JacksonJsonSerde<>(ProcessedOrderEvent.class);

            TestInputTopic<String, OrderCreatedEvent> input =
                    driver.createInputTopic(
                            INPUT_TOPIC,
                            Serdes.String().serializer(),
                            orderSerde.serializer()
                    );

            TestOutputTopic<String, ProcessedOrderEvent> output =
                    driver.createOutputTopic(
                            OUTPUT_TOPIC,
                            Serdes.String().deserializer(),
                            processedSerde.deserializer()
                    );

            // Invalid quantity
            input.pipeInput(
                    "ORD-INVALID-100",
                    new OrderCreatedEvent(
                            "ORD-INVALID-100",
                            "CUST-100",
                            "Laptop",
                            0,
                            "CREATED"
                    )
            );

            // Invalid status
            input.pipeInput(
                    "ORD-INVALID-101",
                    new OrderCreatedEvent(
                            "ORD-INVALID-101",
                            "CUST-101",
                            "Laptop",
                            2,
                            "CANCELLED"
                    )
            );

            assertTrue(output.isEmpty());
        }
    }
}