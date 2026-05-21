package com.global.producer.integration;

import static org.assertj.core.api.Assertions.assertThat;

import com.global.producer.Application;
import com.global.producer.service.FlowLoaderService;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import com.global.producer.service.LaunchService;

@SpringBootTest(
        classes = Application.class,
        properties = {
                "spring.main.web-application-type=none",
                "spring.kafka.bootstrap-servers=localhost:9092"
        })
class ApplicationContextIntegrationTest {

    @TempDir
    static Path tempDir;

    private static Path dataDirectory;

    @DynamicPropertySource
    static void registerProperties(DynamicPropertyRegistry registry) {
        registry.add("app.data-directory", () -> dataDirectory.toString());
    }

    @BeforeAll
    static void setUpDataDirectory() throws Exception {
        dataDirectory = tempDir.resolve("data");
        Path flowDirectory = dataDirectory.resolve("flow-context");
        Files.createDirectories(flowDirectory);
        Files.writeString(flowDirectory.resolve("flow-context.yml"), """
                topic: context.topic
                schedule:
                  duration: 1h
                timestamp:
                  event_time:
                    format: NOW
                    locale: en
                variables:
                  ENV:
                    choice: ["CTX"]
                """);
        Files.writeString(flowDirectory.resolve("message.msg"), "context=${ENV}, ts=${TIMESTAMP}");
    }

    @Autowired
    private FlowLoaderService flowLoaderService;

    @MockitoBean
    private LaunchService launchService;

    @Test
    void contextShouldLoadFlowsFromExternalDataDirectory() {
        var flowDefinitions = flowLoaderService.getAllFlows();
        assertThat(flowDefinitions).hasSize(1);
        assertThat(flowDefinitions.getFirst().getName()).isEqualTo("flow-context");
        assertThat(flowDefinitions.getFirst().getTopic()).isEqualTo("context.topic");
    }
}
