package com.global.producer.service;

import com.global.producer.api.PublishMessageResponse;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class KafkaPublishService {

    private static final long SEND_TIMEOUT_SECONDS = 10L;

    private final KafkaTemplate<String, String> kafkaTemplate;

    public PublishMessageResponse publish(String topic, String key, String value) {
        try {
            SendResult<String, String> sendResult = kafkaTemplate
                    .send(topic, key, value)
                    .get(SEND_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            var recordMetadata = sendResult.getRecordMetadata();
            return new PublishMessageResponse(
                    recordMetadata.topic(),
                    key,
                    value,
                    recordMetadata.partition(),
                    recordMetadata.offset(),
                    recordMetadata.timestamp());
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while publishing message to topic " + topic, exception);
        } catch (ExecutionException | TimeoutException exception) {
            throw new IllegalStateException("Failed to publish message to topic " + topic, exception);
        }
    }
}
