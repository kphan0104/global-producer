package com.global.producer.service;

import static com.global.producer.support.FlowDefinitionTestFactory.choice;
import static com.global.producer.support.FlowDefinitionTestFactory.durationSchedule;
import static com.global.producer.support.FlowDefinitionTestFactory.flowDefinition;
import static com.global.producer.support.FlowDefinitionTestFactory.pattern;
import static com.global.producer.support.FlowDefinitionTestFactory.timestamp;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.global.producer.model.FlowDefinition;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class TemplateRendererServiceTest {

    @TempDir
    Path tempDir;

    private final TemplateRendererService templateRendererService =
            new TemplateRendererService(new SimplePatternGenerator());

    @Test
    void renderShouldReplaceTimestampAndReuseGeneratedValueWithinOneMessage() throws Exception {
        Path messageFile = tempDir.resolve("sample.msg");
        Files.writeString(messageFile, "time=${TIMESTAMP}|code=${CODE}|repeat=${CODE}|env=${ENV}");

        FlowDefinition flowDefinition = flowDefinition(
                "flow-a",
                "topic-a",
                durationSchedule("10s"),
                timestamp("yyyy-MM-dd HH:mm:ss.SSS", "UTC", "en"),
                Map.of(
                        "CODE", pattern("[A-Z]{2}[0-9]{2}"),
                        "ENV", choice("DEV")),
                java.util.List.of(messageFile));

        String rendered = templateRendererService.render(
                flowDefinition,
                messageFile,
                Instant.parse("2026-05-15T21:41:32.866215Z"));

        assertThat(rendered).startsWith("time=2026-05-15 21:41:32.866|code=");
        String[] parts = rendered.split("\\|");
        assertThat(parts[1]).matches("code=[A-Z]{2}[0-9]{2}");
        assertThat(parts[2]).isEqualTo("repeat=" + parts[1].substring("code=".length()));
        assertThat(parts[3]).isEqualTo("env=DEV");
    }

    @Test
    void renderShouldSupportOffsetTimezones() throws Exception {
        Path messageFile = tempDir.resolve("offset.msg");
        Files.writeString(messageFile, "${TIMESTAMP}");

        FlowDefinition flowDefinition = flowDefinition(
                "flow-offset",
                "topic-offset",
                durationSchedule("10s"),
                timestamp("yyyy-MM-dd HH:mm:ss XXX", "+02:00", "en"),
                Map.of(),
                java.util.List.of(messageFile));

        String rendered = templateRendererService.render(
                flowDefinition,
                messageFile,
                Instant.parse("2026-05-15T21:41:32Z"));

        assertThat(rendered).isEqualTo("2026-05-15 23:41:32 +02:00");
    }

    @Test
    void renderShouldSupportAdjacentPlaceholdersAndTrailingLiterals() throws Exception {
        Path messageFile = tempDir.resolve("adjacent.msg");
        Files.writeString(messageFile, "${ENV}${CODE}-tail-${TIMESTAMP}");

        FlowDefinition flowDefinition = flowDefinition(
                "flow-adjacent",
                "topic-adjacent",
                durationSchedule("10s"),
                timestamp("yyyyMMddHHmmss", "UTC", "en"),
                Map.of(
                        "ENV", choice("QA"),
                        "CODE", pattern("[0-9]{4}")),
                java.util.List.of(messageFile));

        String rendered = templateRendererService.render(
                flowDefinition,
                messageFile,
                Instant.parse("2026-05-15T21:41:32Z"));

        assertThat(rendered).matches("QA[0-9]{4}-tail-20260515214132");
    }

    @Test
    void renderShouldCompileLazilyWithoutExplicitValidation() throws Exception {
        Path messageFile = tempDir.resolve("lazy.msg");
        Files.writeString(messageFile, "plain-${TIMESTAMP}");

        FlowDefinition flowDefinition = flowDefinition(
                "flow-lazy",
                "topic-lazy",
                durationSchedule("10s"),
                timestamp("yyyyMMdd", "UTC", "en"),
                Map.of(),
                java.util.List.of(messageFile));

        String rendered = templateRendererService.render(
                flowDefinition,
                messageFile,
                Instant.parse("2026-05-15T21:41:32Z"));

        assertThat(rendered).isEqualTo("plain-20260515");
    }

    @Test
    void validateFlowDefinitionShouldRejectUnknownPlaceholders() throws Exception {
        Path messageFile = tempDir.resolve("unknown.msg");
        Files.writeString(messageFile, "value=${UNKNOWN}");

        FlowDefinition flowDefinition = flowDefinition(
                "flow-invalid",
                "topic-invalid",
                durationSchedule("10s"),
                timestamp("ISO_INSTANT", "UTC", "en"),
                Map.of(),
                java.util.List.of(messageFile));

        assertThatThrownBy(() -> templateRendererService.validateFlowDefinition(flowDefinition))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("UNKNOWN");
    }

    @Test
    void renderShouldUseCompiledSnapshotUntilFlowIsRevalidated() throws Exception {
        Path messageFile = tempDir.resolve("cached.msg");
        Files.writeString(messageFile, "env=${ENV}|ts=${TIMESTAMP}");

        FlowDefinition initialFlowDefinition = flowDefinition(
                "flow-cache",
                "topic-cache",
                durationSchedule("10s"),
                timestamp("yyyy-MM-dd HH:mm:ss", "UTC", "en"),
                Map.of("ENV", choice("DEV")),
                java.util.List.of(messageFile));

        templateRendererService.validateFlowDefinition(initialFlowDefinition);
        Files.writeString(messageFile, "env=${ENV}|ts=${TIMESTAMP}|version=2");

        String renderedWithCachedSnapshot = templateRendererService.render(
                initialFlowDefinition,
                messageFile,
                Instant.parse("2026-05-15T21:41:32Z"));

        assertThat(renderedWithCachedSnapshot).isEqualTo("env=DEV|ts=2026-05-15 21:41:32");

        FlowDefinition reloadedFlowDefinition = flowDefinition(
                "flow-cache",
                "topic-cache",
                durationSchedule("10s"),
                timestamp("yyyy-MM-dd HH:mm:ss", "UTC", "en"),
                Map.of("ENV", choice("QA")),
                java.util.List.of(messageFile));

        templateRendererService.validateFlowDefinition(reloadedFlowDefinition);

        String renderedAfterRevalidation = templateRendererService.render(
                reloadedFlowDefinition,
                messageFile,
                Instant.parse("2026-05-15T21:41:32Z"));

        assertThat(renderedAfterRevalidation).isEqualTo("env=QA|ts=2026-05-15 21:41:32|version=2");
    }
}
