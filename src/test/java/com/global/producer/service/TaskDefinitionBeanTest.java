package com.global.producer.service;

import static com.global.producer.support.FlowDefinitionTestFactory.choice;
import static com.global.producer.support.FlowDefinitionTestFactory.durationSchedule;
import static com.global.producer.support.FlowDefinitionTestFactory.flowDefinition;
import static com.global.producer.support.FlowDefinitionTestFactory.singleTimestampProfile;
import static com.global.producer.support.FlowDefinitionTestFactory.timestamp;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.global.producer.model.FlowDefinition;
import com.global.producer.property.AppProperties;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.concurrent.CompletableFuture;
import java.util.Map;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.ArgumentCaptor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

class TaskDefinitionBeanTest {

    @TempDir
    Path tempDir;

    @Test
    void runShouldRenderPayloadAndSendTimestampedKafkaRecord() throws Exception {
        Path messageFile = tempDir.resolve("message.msg");
        Files.writeString(messageFile, "time=${TIMESTAMP}|env=${ENV}");

        FlowDefinition flowDefinition = flowDefinition(
                "flow-a",
                "topic-a",
                durationSchedule("10s"),
                singleTimestampProfile("event_time", timestamp("yyyy-MM-dd HH:mm:ss", "UTC", "en")),
                Map.of("ENV", choice("QA")),
                java.util.List.of(messageFile));

        @SuppressWarnings("unchecked")
        KafkaTemplate<String, String> kafkaTemplate = org.mockito.Mockito.mock(KafkaTemplate.class);
        SendResult<String, String> sendResult = new SendResult<>(
                new ProducerRecord<>("topic-a", "time=2026-05-15 21:41:32|env=QA"),
                new RecordMetadata(
                        new org.apache.kafka.common.TopicPartition("topic-a", 1),
                        42,
                        0,
                        Instant.parse("2026-05-15T21:41:32Z").toEpochMilli(),
                        0,
                        0));
        when(kafkaTemplate.send(any(ProducerRecord.class)))
                .thenReturn(CompletableFuture.completedFuture(sendResult));

        TaskDefinitionBean taskDefinitionBean = new TaskDefinitionBean(
                flowDefinition,
                new TemplateRendererService(new SimplePatternGenerator()),
                databusPayloadService(),
                kafkaTemplate,
                Clock.fixed(Instant.parse("2026-05-15T21:41:32Z"), ZoneOffset.UTC));

        taskDefinitionBean.run();

        @SuppressWarnings({"rawtypes", "unchecked"})
        ArgumentCaptor<ProducerRecord<String, String>> recordCaptor = ArgumentCaptor.forClass((Class) ProducerRecord.class);
        verify(kafkaTemplate).send(recordCaptor.capture());

        ProducerRecord<String, String> record = recordCaptor.getValue();
        assertThat(record.topic()).isEqualTo("topic-a");
        assertThat(record.key()).isNull();
        assertThat(record.timestamp()).isEqualTo(Instant.parse("2026-05-15T21:41:32Z").toEpochMilli());
        assertThat(record.value()).isEqualTo(
                "{\"databus.flow.name\":\"flow-a\","
                        + "\"databus.flow.provider.name\":\"integration-tests\","
                        + "\"originalMessage\":\"time=2026-05-15 21:41:32|env=QA\","
                        + "\"databus.event.lineage.stage1.timestamp\":\"2026-05-15T21:41:32Z\","
                        + "\"databus.event.lineage.stage1.pipeline_id\":\"global_producer\"}");
    }

    @Test
    void runShouldWrapRenderingErrors() {
        FlowDefinition flowDefinition = flowDefinition(
                "flow-a",
                "topic-a",
                durationSchedule("10s"),
                singleTimestampProfile("event_time", timestamp("yyyy-MM-dd HH:mm:ss", "UTC", "en")),
                Map.of("ENV", choice("QA")),
                java.util.List.of(tempDir.resolve("missing.msg")));

        @SuppressWarnings("unchecked")
        KafkaTemplate<String, String> kafkaTemplate = org.mockito.Mockito.mock(KafkaTemplate.class);

        TaskDefinitionBean taskDefinitionBean = new TaskDefinitionBean(
                flowDefinition,
                new TemplateRendererService(new SimplePatternGenerator()),
                databusPayloadService(),
                kafkaTemplate,
                Clock.fixed(Instant.parse("2026-05-15T21:41:32Z"), ZoneOffset.UTC));

        assertThatThrownBy(taskDefinitionBean::run)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Unable to produce flow flow-a");
    }

    @Test
    void runShouldNotThrowWhenKafkaSendCompletesExceptionally() throws Exception {
        Path messageFile = tempDir.resolve("message.msg");
        Files.writeString(messageFile, "time=${TIMESTAMP}|env=${ENV}");

        FlowDefinition flowDefinition = flowDefinition(
                "flow-a",
                "topic-a",
                durationSchedule("10s"),
                singleTimestampProfile("event_time", timestamp("yyyy-MM-dd HH:mm:ss", "UTC", "en")),
                Map.of("ENV", choice("QA")),
                java.util.List.of(messageFile));

        @SuppressWarnings("unchecked")
        KafkaTemplate<String, String> kafkaTemplate = org.mockito.Mockito.mock(KafkaTemplate.class);
        when(kafkaTemplate.send(any(ProducerRecord.class)))
                .thenReturn(CompletableFuture.failedFuture(new RuntimeException("broker down")));

        TaskDefinitionBean taskDefinitionBean = new TaskDefinitionBean(
                flowDefinition,
                new TemplateRendererService(new SimplePatternGenerator()),
                databusPayloadService(),
                kafkaTemplate,
                Clock.fixed(Instant.parse("2026-05-15T21:41:32Z"), ZoneOffset.UTC));

        assertThatCode(taskDefinitionBean::run).doesNotThrowAnyException();
        verify(kafkaTemplate).send(any(ProducerRecord.class));
    }

    private DatabusPayloadService databusPayloadService() {
        AppProperties appProperties = new AppProperties();
        appProperties.getDatabus().getFlow().getProvider().setName("integration-tests");
        return new DatabusPayloadService(new com.fasterxml.jackson.databind.ObjectMapper(), appProperties);
    }
}
