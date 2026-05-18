package com.global.producer.controller;

import com.global.producer.api.PublishMessageRequest;
import com.global.producer.api.PublishMessageResponse;
import com.global.producer.service.KafkaPublishService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/messages")
@RequiredArgsConstructor
public class KafkaPublishController {

    private final KafkaPublishService kafkaPublishService;

    @PostMapping
    public ResponseEntity<PublishMessageResponse> publish(@Valid @RequestBody PublishMessageRequest request) {
        return ResponseEntity.ok(kafkaPublishService.publish(request.topic(), request.key(), request.value()));
    }
}
