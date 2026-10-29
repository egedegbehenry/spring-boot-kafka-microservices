package com.henry.consumer.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

@Configuration
public class KafkaConsumerConfiguration {
    private static final Logger log = LoggerFactory.getLogger(KafkaConsumerConfiguration.class);

    @Bean
    public DefaultErrorHandler kafkaErrorHandler() {
        // Retry transient listener failures twice, then log recovery and continue to subsequent orders.
        DefaultErrorHandler handler = new DefaultErrorHandler((record, failure) ->
                log.error("Order recovery: skipping failed record topic={} partition={} offset={} key={}",
                        record.topic(), record.partition(), record.offset(), record.key(), failure),
                new FixedBackOff(1000L, 2L));
        // Invalid event data will not improve on retry, so send it straight to logging recovery.
        handler.addNotRetryableExceptions(IllegalArgumentException.class);
        return handler;
    }
}
