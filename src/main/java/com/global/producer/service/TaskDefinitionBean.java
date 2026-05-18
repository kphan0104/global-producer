package com.global.producer.service;

import com.global.producer.model.FlowDefinition;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.kafka.core.KafkaTemplate;

@Slf4j
public record TaskDefinitionBean(
        FlowDefinition flowDefinition,
        TemplateRendererService templateRendererService,
        KafkaTemplate<String, String> kafkaTemplate,
        Clock clock) implements Runnable {

    @Override
    public void run() {
        Path messageFile = selectRandomMessageFile();
        Instant now = Instant.now(clock);
        try {
            String payload = templateRendererService.render(flowDefinition, messageFile, now);
            ProducerRecord<String, String> producerRecord =
                    new ProducerRecord<>(flowDefinition.getTopic(), null, now.toEpochMilli(), null, payload);

            kafkaTemplate.send(producerRecord).whenComplete((sendResult, throwable) -> {
                if (throwable != null) {
                    log.error(
                            "Failed to produce flow {} using template {} on topic {}",
                            flowDefinition.getName(),
                            messageFile.getFileName(),
                            flowDefinition.getTopic(),
                            throwable);
                    return;
                }

                var recordMetadata = sendResult.getRecordMetadata();
                log.info(
                        "Produced flow {} using template {} on topic {} partition {} offset {}",
                        flowDefinition.getName(),
                        messageFile.getFileName(),
                        flowDefinition.getTopic(),
                        recordMetadata.partition(),
                        recordMetadata.offset());
            });
        } catch (UncheckedIOException | IllegalArgumentException exception) {
            throw new IllegalStateException(
                    "Unable to produce flow " + flowDefinition.getName() + " from template " + messageFile,
                    exception);
        }
    }

    private Path selectRandomMessageFile() {
        List<Path> messageFiles = flowDefinition.getMessageFiles();
        return messageFiles.get(ThreadLocalRandom.current().nextInt(messageFiles.size()));
    }
}
