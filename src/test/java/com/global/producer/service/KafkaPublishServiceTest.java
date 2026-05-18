package com.global.producer.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.when;

import com.global.producer.api.PublishMessageResponse;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

class KafkaPublishServiceTest {

    @Test
    void shouldReturnKafkaMetadataWhenPublishSucceeds() {
        @SuppressWarnings("unchecked")
        KafkaTemplate<String, String> kafkaTemplate = Mockito.mock(KafkaTemplate.class);
        KafkaPublishService service = new KafkaPublishService(kafkaTemplate);

        ProducerRecord<String, String> producerRecord = new ProducerRecord<>("topic-a", "key-a", "value-a");
        RecordMetadata recordMetadata = new RecordMetadata(
                new TopicPartition("topic-a", 2),
                0L,
                15,
                Instant.parse("2026-05-18T00:00:00Z").toEpochMilli(),
                5,
                7);
        SendResult<String, String> sendResult = new SendResult<>(producerRecord, recordMetadata);

        when(kafkaTemplate.send("topic-a", "key-a", "value-a"))
                .thenReturn(CompletableFuture.completedFuture(sendResult));

        PublishMessageResponse response = service.publish("topic-a", "key-a", "value-a");

        assertThat(response.topic()).isEqualTo("topic-a");
        assertThat(response.key()).isEqualTo("key-a");
        assertThat(response.value()).isEqualTo("value-a");
        assertThat(response.partition()).isEqualTo(2);
        assertThat(response.offset()).isEqualTo(15L);
        assertThat(response.timestamp()).isEqualTo(Instant.parse("2026-05-18T00:00:00Z").toEpochMilli());
    }

    @Test
    void shouldWrapKafkaFailures() {
        @SuppressWarnings("unchecked")
        KafkaTemplate<String, String> kafkaTemplate = Mockito.mock(KafkaTemplate.class);
        KafkaPublishService service = new KafkaPublishService(kafkaTemplate);

        when(kafkaTemplate.send("topic-a", "key-a", "value-a"))
                .thenReturn(CompletableFuture.failedFuture(new RuntimeException("broker down")));

        assertThatThrownBy(() -> service.publish("topic-a", "key-a", "value-a"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("topic-a");
    }
}
