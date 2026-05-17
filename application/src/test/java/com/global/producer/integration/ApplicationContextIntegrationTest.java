package com.global.producer.integration;

import static org.assertj.core.api.Assertions.assertThat;

import com.global.producer.Application;
import com.global.producer.service.TaskDefinitionBean;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
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
                  format: ISO_INSTANT
                  timezone: UTC
                  locale: en
                variables:
                  ENV:
                    choice: ["CTX"]
                """);
        Files.writeString(flowDirectory.resolve("message.msg"), "context=${ENV}, ts=${TIMESTAMP}");
    }

    @Autowired
    private List<TaskDefinitionBean> taskDefinitionBeans;

    @MockitoBean
    private LaunchService launchService;

    @Test
    void contextShouldLoadAndCreateTaskDefinitionsFromExternalDataDirectory() {
        assertThat(taskDefinitionBeans).hasSize(1);
        assertThat(taskDefinitionBeans.getFirst().flowDefinition().getName()).isEqualTo("flow-context");
        assertThat(taskDefinitionBeans.getFirst().flowDefinition().getTopic()).isEqualTo("context.topic");
    }
}
