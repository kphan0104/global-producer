package com.global.producer.api;

import io.swagger.v3.oas.annotations.media.Schema;

public record PublishMessageResponse(
        @Schema(description = "Kafka topic name")
        String topic,
        @Schema(description = "Kafka message key")
        String key,
        @Schema(description = "Kafka payload sent as-is")
        String value,
        @Schema(description = "Kafka partition")
        int partition,
        @Schema(description = "Kafka offset")
        long offset,
        @Schema(description = "Kafka record timestamp in epoch milliseconds")
        long timestamp) {
}
