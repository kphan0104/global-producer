package com.global.producer.integration;

import static org.assertj.core.api.Assertions.assertThat;

import com.global.producer.model.FlowDefinition;
import com.global.producer.property.AppProperties;
import com.global.producer.service.FlowLoaderService;
import com.global.producer.service.SimplePatternGenerator;
import com.global.producer.service.TaskDefinitionBean;
import com.global.producer.service.TemplateRendererService;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

@SpringJUnitConfig(classes = FlowKafkaIntegrationTest.EmptyConfiguration.class)
@EmbeddedKafka(partitions = 1, topics = "flow.integration")
class FlowKafkaIntegrationTest {

    @Configuration
    static class EmptyConfiguration {
    }

    @Autowired
    private EmbeddedKafkaBroker embeddedKafkaBroker;

    @TempDir
    Path tempDir;

    @Test
    void shouldLoadFlowAndProduceRenderedMessageToKafka() throws Exception {
        Path dataDirectory = tempDir.resolve("data");
        Path flowDirectory = dataDirectory.resolve("flow-integration");
        Files.createDirectories(flowDirectory);
        Files.writeString(flowDirectory.resolve("flow-integration.yml"), """
                topic: flow.integration
                schedule:
                  duration: 10s
                timestamp:
                  event_time:
                    format: NOW
                    locale: en
                variables:
                  ENV:
                    choice: ["INT"]
                """);
        Files.writeString(flowDirectory.resolve("message.msg"), "{\"ts\":\"${TIMESTAMP:event_time}\",\"env\":\"${ENV}\"}");

        TemplateRendererService templateRendererService = new TemplateRendererService(new SimplePatternGenerator());
        AppProperties appProperties = new AppProperties();
        appProperties.setDataDirectory(dataDirectory);
        FlowLoaderService flowLoaderService = new FlowLoaderService(appProperties, templateRendererService);

        FlowDefinition flowDefinition = flowLoaderService.getAllFlows().getFirst();

        Map<String, Object> producerProperties = Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, embeddedKafkaBroker.getBrokersAsString(),
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.ACKS_CONFIG, "all");

        DefaultKafkaProducerFactory<String, String> producerFactory =
                new DefaultKafkaProducerFactory<>(producerProperties);

        try (Consumer<String, String> consumer = createConsumer()) {
            KafkaTemplate<String, String> kafkaTemplate = new KafkaTemplate<>(producerFactory);
            TaskDefinitionBean taskDefinitionBean = new TaskDefinitionBean(
                    flowDefinition,
                    templateRendererService,
                    kafkaTemplate,
                    Clock.fixed(Instant.parse("2026-05-15T21:41:32.866Z"), ZoneOffset.UTC));

            consumer.subscribe(List.of("flow.integration"));
            consumer.poll(Duration.ofMillis(200));

            taskDefinitionBean.run();
            kafkaTemplate.flush();

            ConsumerRecord<String, String> record = pollSingleRecord(consumer, Duration.ofSeconds(10));
            assertThat(record.topic()).isEqualTo("flow.integration");
            assertThat(record.timestamp()).isEqualTo(Instant.parse("2026-05-15T21:41:32.866Z").toEpochMilli());
            assertThat(record.value()).isEqualTo("{\"ts\":\"2026-05-15T21:41:32.866Z\",\"env\":\"INT\"}");
        } finally {
            producerFactory.destroy();
        }
    }

    private Consumer<String, String> createConsumer() {
        return new KafkaConsumer<>(Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, embeddedKafkaBroker.getBrokersAsString(),
                ConsumerConfig.GROUP_ID_CONFIG, "flow-integration-test",
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class));
    }

    private ConsumerRecord<String, String> pollSingleRecord(Consumer<String, String> consumer, Duration timeout) {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadline) {
            var records = consumer.poll(Duration.ofMillis(250));
            if (!records.isEmpty()) {
                return records.iterator().next();
            }
        }
        throw new AssertionError("No Kafka record received within " + timeout);
    }
}
