package com.global.producer.controller;

import com.global.producer.api.PublishMessageRequest;
import com.global.producer.api.PublishMessageResponse;
import com.global.producer.service.KafkaPublishService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.ExampleObject;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
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
@Tag(name = "Kafka Publish", description = "Direct Kafka publishing without Databus flow enrichment.")
public class KafkaPublishController {

    private final KafkaPublishService kafkaPublishService;

    @PostMapping
    @Operation(
            summary = "Publish a direct Kafka message",
            description = "Publishes the provided topic, key and value as-is, without the Databus scheduler envelope.")
    @ApiResponses({
            @ApiResponse(
                    responseCode = "200",
                    description = "Message published successfully",
                    content = @Content(
                            mediaType = "application/json",
                            schema = @Schema(implementation = PublishMessageResponse.class))),
            @ApiResponse(responseCode = "400", description = "Invalid request payload")
    })
    public ResponseEntity<PublishMessageResponse> publish(@Valid @RequestBody PublishMessageRequest request) {
        return ResponseEntity.ok(kafkaPublishService.publish(request.topic(), request.key(), request.value()));
    }
}
