package com.global.producer.api;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

public record PublishMessageRequest(
        @Schema(description = "Kafka topic name", example = "demo.manual")
        @NotBlank String topic,
        @Schema(description = "Kafka message key", example = "customer-42", nullable = true)
        @NotNull String key,
        @Schema(description = "Kafka payload sent as-is", example = "{\"status\":\"OK\"}")
        @NotNull String value) {
}
