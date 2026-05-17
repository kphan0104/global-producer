package com.global.producer.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.global.producer.property.AppProperties;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class FlowLoaderServiceTest {

    @TempDir
    Path tempDir;

    private final TemplateRendererService templateRendererService =
            new TemplateRendererService(new SimplePatternGenerator());

    @Test
    void getAllFlowsShouldLoadAndSortFlowDirectories() throws Exception {
        Path dataDirectory = tempDir.resolve("data");
        createFlow(dataDirectory.resolve("flow-b"), "flow-b", "topic-b");
        createFlow(dataDirectory.resolve("flow-a"), "flow-a", "topic-a");

        AppProperties appProperties = new AppProperties();
        appProperties.setDataDirectory(dataDirectory);

        FlowLoaderService service = new FlowLoaderService(appProperties, templateRendererService);

        var flows = service.getAllFlows();

        assertThat(flows).hasSize(2);
        assertThat(flows).extracting("name").containsExactly("flow-a", "flow-b");
        assertThat(flows.get(0).getMessageFiles()).hasSize(1);
        assertThat(flows.get(0).getTopic()).isEqualTo("topic-a");
    }

    @Test
    void getAllFlowsShouldFailWhenConfigFileIsMissing() throws Exception {
        Path dataDirectory = tempDir.resolve("data");
        Path flowDirectory = dataDirectory.resolve("flow-a");
        Files.createDirectories(flowDirectory);
        Files.writeString(flowDirectory.resolve("message.msg"), "value=${TIMESTAMP}");

        AppProperties appProperties = new AppProperties();
        appProperties.setDataDirectory(dataDirectory);

        FlowLoaderService service = new FlowLoaderService(appProperties, templateRendererService);

        assertThatThrownBy(service::getAllFlows)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("flow-a.yml");
    }

    @Test
    void getAllFlowsShouldSupportYamlExtension() throws Exception {
        Path dataDirectory = tempDir.resolve("data");
        Path flowDirectory = dataDirectory.resolve("flow-a");
        Files.createDirectories(flowDirectory);
        Files.writeString(flowDirectory.resolve("flow-a.yaml"), """
                topic: topic-a
                schedule:
                  duration: 10s
                timestamp:
                  format: ISO_INSTANT
                  timezone: UTC
                  locale: en
                """);
        Files.writeString(flowDirectory.resolve("message.msg"), "value=${TIMESTAMP}");

        AppProperties appProperties = new AppProperties();
        appProperties.setDataDirectory(dataDirectory);

        FlowLoaderService service = new FlowLoaderService(appProperties, templateRendererService);

        var flows = service.getAllFlows();

        assertThat(flows).hasSize(1);
        assertThat(flows.getFirst().getName()).isEqualTo("flow-a");
        assertThat(flows.getFirst().getTopic()).isEqualTo("topic-a");
    }

    @Test
    void getAllFlowsShouldFailWhenBothYmlAndYamlExist() throws Exception {
        Path dataDirectory = tempDir.resolve("data");
        Path flowDirectory = dataDirectory.resolve("flow-a");
        Files.createDirectories(flowDirectory);
        Files.writeString(flowDirectory.resolve("flow-a.yml"), """
                topic: topic-a
                schedule:
                  duration: 10s
                timestamp:
                  format: ISO_INSTANT
                  timezone: UTC
                  locale: en
                """);
        Files.writeString(flowDirectory.resolve("flow-a.yaml"), """
                topic: topic-a
                schedule:
                  duration: 10s
                timestamp:
                  format: ISO_INSTANT
                  timezone: UTC
                  locale: en
                """);
        Files.writeString(flowDirectory.resolve("message.msg"), "value=${TIMESTAMP}");

        AppProperties appProperties = new AppProperties();
        appProperties.setDataDirectory(dataDirectory);

        FlowLoaderService service = new FlowLoaderService(appProperties, templateRendererService);

        assertThatThrownBy(service::getAllFlows)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("both .yml and .yaml");
    }

    @Test
    void getAllFlowsShouldFailWhenNoFlowDirectoriesExist() throws Exception {
        Path dataDirectory = tempDir.resolve("data");
        Files.createDirectories(dataDirectory);

        AppProperties appProperties = new AppProperties();
        appProperties.setDataDirectory(dataDirectory);

        FlowLoaderService service = new FlowLoaderService(appProperties, templateRendererService);

        assertThatThrownBy(service::getAllFlows)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("No flow folders found");
    }

    private void createFlow(Path flowDirectory, String flowName, String topic) throws Exception {
        Files.createDirectories(flowDirectory);
        Files.writeString(flowDirectory.resolve(flowName + ".yml"), """
                topic: %s
                schedule:
                  duration: 10s
                timestamp:
                  format: ISO_INSTANT
                  timezone: UTC
                  locale: en
                variables:
                  ENV:
                    choice: ["DEV"]
                """.formatted(topic));
        Files.writeString(flowDirectory.resolve("message.msg"), "value=${TIMESTAMP}-${ENV}");
    }
}
