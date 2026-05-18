package com.global.producer.api;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

public record PublishMessageRequest(
        @NotBlank String topic,
        @NotNull String key,
        @NotNull String value) {
}
