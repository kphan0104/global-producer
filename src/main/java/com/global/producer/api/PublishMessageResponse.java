package com.global.producer.api;

public record PublishMessageResponse(
        String topic,
        String key,
        String value,
        int partition,
        long offset,
        long timestamp) {
}
