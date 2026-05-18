package com.global.producer.service;

import static com.global.producer.support.FlowDefinitionTestFactory.choice;
import static com.global.producer.support.FlowDefinitionTestFactory.durationSchedule;
import static com.global.producer.support.FlowDefinitionTestFactory.flowDefinition;
import static com.global.producer.support.FlowDefinitionTestFactory.pattern;
import static com.global.producer.support.FlowDefinitionTestFactory.singleTimestampProfile;
import static com.global.producer.support.FlowDefinitionTestFactory.timestamp;
import static com.global.producer.support.FlowDefinitionTestFactory.timestampProfile;
import static com.global.producer.support.FlowDefinitionTestFactory.timestampProfiles;
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
    void renderShouldReplaceNamedTimestampProfilesAndReuseGeneratedValueWithinOneMessage() throws Exception {
        Path messageFile = tempDir.resolve("sample.msg");
        Files.writeString(
                messageFile,
                "utc=${TIMESTAMP:event_time}|local=${TIMESTAMP:paris_time}|code=${CODE}|repeat=${CODE}|env=${ENV}");

        FlowDefinition flowDefinition = flowDefinition(
                "flow-a",
                "topic-a",
                durationSchedule("10s"),
                timestampProfiles(
                        timestampProfile("event_time", timestamp("NOW", null, "en")),
                        timestampProfile("paris_time", timestamp("yyyy-MM-dd HH:mm:ss.SSS", "Europe/Paris", "en"))),
                Map.of(
                        "CODE", pattern("[A-Z]{2}[0-9]{2}"),
                        "ENV", choice("DEV")),
                java.util.List.of(messageFile));

        String rendered = templateRendererService.render(
                flowDefinition,
                messageFile,
                Instant.parse("2026-05-15T21:41:32.866215Z"));

        String[] parts = rendered.split("\\|");
        assertThat(parts[0]).isEqualTo("utc=2026-05-15T21:41:32.866215Z");
        assertThat(parts[1]).isEqualTo("local=2026-05-15 23:41:32.866");
        assertThat(parts[2]).matches("code=[A-Z]{2}[0-9]{2}");
        assertThat(parts[3]).isEqualTo("repeat=" + parts[2].substring("code=".length()));
        assertThat(parts[4]).isEqualTo("env=DEV");
    }

    @Test
    void renderShouldSupportOffsetTimezones() throws Exception {
        Path messageFile = tempDir.resolve("offset.msg");
        Files.writeString(messageFile, "${TIMESTAMP:offset_time}");

        FlowDefinition flowDefinition = flowDefinition(
                "flow-offset",
                "topic-offset",
                durationSchedule("10s"),
                singleTimestampProfile("offset_time", timestamp("yyyy-MM-dd HH:mm:ss XXX", "+02:00", "en")),
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
        Files.writeString(messageFile, "${ENV}${CODE}-tail-${TIMESTAMP:event_time}");

        FlowDefinition flowDefinition = flowDefinition(
                "flow-adjacent",
                "topic-adjacent",
                durationSchedule("10s"),
                singleTimestampProfile("event_time", timestamp("yyyyMMddHHmmss", "UTC", "en")),
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
        Files.writeString(messageFile, "plain-${TIMESTAMP:event_time}");

        FlowDefinition flowDefinition = flowDefinition(
                "flow-lazy",
                "topic-lazy",
                durationSchedule("10s"),
                singleTimestampProfile("event_time", timestamp("yyyyMMdd", "UTC", "en")),
                Map.of(),
                java.util.List.of(messageFile));

        String rendered = templateRendererService.render(
                flowDefinition,
                messageFile,
                Instant.parse("2026-05-15T21:41:32Z"));

        assertThat(rendered).isEqualTo("plain-20260515");
    }

    @Test
    void validateFlowDefinitionShouldRejectUnknownVariablePlaceholders() throws Exception {
        Path messageFile = tempDir.resolve("unknown.msg");
        Files.writeString(messageFile, "value=${UNKNOWN}");

        FlowDefinition flowDefinition = flowDefinition(
                "flow-invalid",
                "topic-invalid",
                durationSchedule("10s"),
                singleTimestampProfile("event_time", timestamp("NOW", null, "en")),
                Map.of(),
                java.util.List.of(messageFile));

        assertThatThrownBy(() -> templateRendererService.validateFlowDefinition(flowDefinition))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("UNKNOWN");
    }

    @Test
    void validateFlowDefinitionShouldRejectBareTimestampPlaceholder() throws Exception {
        Path messageFile = tempDir.resolve("bare-timestamp.msg");
        Files.writeString(messageFile, "value=${TIMESTAMP}");

        FlowDefinition flowDefinition = flowDefinition(
                "flow-invalid",
                "topic-invalid",
                durationSchedule("10s"),
                singleTimestampProfile("event_time", timestamp("NOW", null, "en")),
                Map.of(),
                java.util.List.of(messageFile));

        assertThatThrownBy(() -> templateRendererService.validateFlowDefinition(flowDefinition))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("without a timestamp profile");
    }

    @Test
    void renderShouldUseCompiledSnapshotUntilFlowIsRevalidated() throws Exception {
        Path messageFile = tempDir.resolve("cached.msg");
        Files.writeString(messageFile, "env=${ENV}|ts=${TIMESTAMP:event_time}");

        FlowDefinition initialFlowDefinition = flowDefinition(
                "flow-cache",
                "topic-cache",
                durationSchedule("10s"),
                singleTimestampProfile("event_time", timestamp("yyyy-MM-dd HH:mm:ss", "UTC", "en")),
                Map.of("ENV", choice("DEV")),
                java.util.List.of(messageFile));

        templateRendererService.validateFlowDefinition(initialFlowDefinition);
        Files.writeString(messageFile, "env=${ENV}|ts=${TIMESTAMP:event_time}|version=2");

        String renderedWithCachedSnapshot = templateRendererService.render(
                initialFlowDefinition,
                messageFile,
                Instant.parse("2026-05-15T21:41:32Z"));

        assertThat(renderedWithCachedSnapshot).isEqualTo("env=DEV|ts=2026-05-15 21:41:32");

        FlowDefinition reloadedFlowDefinition = flowDefinition(
                "flow-cache",
                "topic-cache",
                durationSchedule("10s"),
                singleTimestampProfile("event_time", timestamp("yyyy-MM-dd HH:mm:ss", "UTC", "en")),
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
