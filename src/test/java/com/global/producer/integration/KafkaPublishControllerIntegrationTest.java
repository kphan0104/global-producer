package com.global.producer.integration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.global.producer.Application;
import com.global.producer.api.PublishMessageRequest;
import com.global.producer.service.LaunchService;
import java.time.Duration;
import java.util.Map;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

@SpringBootTest(classes = Application.class)
@AutoConfigureMockMvc
@EmbeddedKafka(partitions = 1, topics = "api.integration")
class KafkaPublishControllerIntegrationTest {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private EmbeddedKafkaBroker embeddedKafkaBroker;

    @MockitoBean
    private LaunchService launchService;

    @DynamicPropertySource
    static void registerProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", () -> System.getProperty("spring.embedded.kafka.brokers"));
    }

    @Test
    void shouldPublishTopicKeyAndValueThroughRestEndpoint() throws Exception {
        try (Consumer<String, String> consumer = createConsumer()) {
            consumer.subscribe(java.util.List.of("api.integration"));
            consumer.poll(Duration.ofMillis(200));

            mockMvc.perform(post("/api/messages")
                            .contentType("application/json")
                            .content(OBJECT_MAPPER.writeValueAsString(
                                    new PublishMessageRequest("api.integration", "customer-42", "{\"status\":\"OK\"}"))))
                    .andExpect(status().isOk())
                    .andExpect(jsonPath("$.topic").value("api.integration"))
                    .andExpect(jsonPath("$.key").value("customer-42"))
                    .andExpect(jsonPath("$.value").value("{\"status\":\"OK\"}"));

            ConsumerRecord<String, String> record = pollSingleRecord(consumer, Duration.ofSeconds(10));
            assertThat(record.topic()).isEqualTo("api.integration");
            assertThat(record.key()).isEqualTo("customer-42");
            assertThat(record.value()).isEqualTo("{\"status\":\"OK\"}");
        }
    }

    @Test
    void shouldRejectInvalidPayload() throws Exception {
        mockMvc.perform(post("/api/messages")
                        .contentType("application/json")
                        .content("""
                                {
                                  "topic": " ",
                                  "key": "customer-42",
                                  "value": "payload"
                                }
                                """))
                .andExpect(status().isBadRequest());
    }

    private Consumer<String, String> createConsumer() {
        return new KafkaConsumer<>(Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, embeddedKafkaBroker.getBrokersAsString(),
                ConsumerConfig.GROUP_ID_CONFIG, "api-integration-test",
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
