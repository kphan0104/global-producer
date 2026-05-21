package com.global.producer.service;

import static com.global.producer.support.FlowDefinitionTestFactory.cronSchedule;
import static com.global.producer.support.FlowDefinitionTestFactory.durationSchedule;
import static com.global.producer.support.FlowDefinitionTestFactory.flowDefinition;
import static com.global.producer.support.FlowDefinitionTestFactory.singleTimestampProfile;
import static com.global.producer.support.FlowDefinitionTestFactory.timestamp;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.global.producer.model.FlowDefinition;
import com.global.producer.property.AppProperties;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mockito;
import org.springframework.kafka.core.KafkaTemplate;

class LaunchServiceTest {

    @TempDir
    Path tempDir;

    @Test
    void reloadFlowsShouldSchedulePeriodicAndCronFlows() {
        FlowLoaderService flowLoaderService = Mockito.mock(FlowLoaderService.class);
        TaskSchedulingService taskSchedulingService = Mockito.mock(TaskSchedulingService.class);
        TemplateRendererService templateRendererService = new TemplateRendererService(new SimplePatternGenerator());
        @SuppressWarnings("unchecked")
        KafkaTemplate<String, String> kafkaTemplate = Mockito.mock(KafkaTemplate.class);

        FlowDefinition durationFlow = flowDefinition(
                "flow-duration",
                "topic-duration",
                durationSchedule("10s"),
                singleTimestampProfile("event_time", timestamp("NOW", null, "en")),
                Map.of(),
                List.of(java.nio.file.Path.of("duration.msg")));
        FlowDefinition cronFlow = flowDefinition(
                "flow-cron",
                "topic-cron",
                cronSchedule("0 */1 * * * *", "Europe/Paris"),
                singleTimestampProfile("event_time", timestamp("NOW", null, "en")),
                Map.of(),
                List.of(java.nio.file.Path.of("cron.msg")));

        when(flowLoaderService.getAllFlows()).thenReturn(List.of(durationFlow, cronFlow));

        LaunchService service = new LaunchService(
                flowLoaderService,
                templateRendererService,
                databusPayloadService(),
                kafkaTemplate,
                Clock.system(ZoneOffset.UTC),
                taskSchedulingService,
                appProperties(tempDir));
        service.reloadFlows();

        verify(taskSchedulingService).scheduleAPeriodicTask(
                eq("flow-duration"),
                any(TaskDefinitionBean.class),
                eq(Duration.ofSeconds(10)));
        verify(taskSchedulingService).scheduleACronTask(
                eq("flow-cron"),
                any(TaskDefinitionBean.class),
                eq("0 */1 * * * *"),
                eq("Europe/Paris"));
    }

    @Test
    void reloadFlowsShouldRejectCronTimezoneInferenceWhenOnlyTimestampProfileUsesNow() {
        FlowLoaderService flowLoaderService = Mockito.mock(FlowLoaderService.class);
        TaskSchedulingService taskSchedulingService = Mockito.mock(TaskSchedulingService.class);
        TemplateRendererService templateRendererService = new TemplateRendererService(new SimplePatternGenerator());
        @SuppressWarnings("unchecked")
        KafkaTemplate<String, String> kafkaTemplate = Mockito.mock(KafkaTemplate.class);

        FlowDefinition cronFlow = flowDefinition(
                "flow-cron",
                "topic-cron",
                cronSchedule("0 */1 * * * *"),
                singleTimestampProfile("event_time", timestamp("NOW", "Europe/Paris", "en")),
                Map.of(),
                List.of(java.nio.file.Path.of("cron.msg")));

        when(flowLoaderService.getAllFlows()).thenReturn(List.of(cronFlow));

        LaunchService service = new LaunchService(
                flowLoaderService,
                templateRendererService,
                databusPayloadService(),
                kafkaTemplate,
                Clock.system(ZoneOffset.UTC),
                taskSchedulingService,
                appProperties(tempDir));

        assertThatThrownBy(service::reloadFlows)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("configure schedule.timezone");
    }

    @Test
    void initializeShouldReloadFlowsWhenTemplatesChange() throws Exception {
        Path dataDirectory = tempDir.resolve("data");
        Path flowDirectory = dataDirectory.resolve("flow-a");
        Files.createDirectories(flowDirectory);
        Files.writeString(flowDirectory.resolve("flow-a.yml"), """
                topic: topic-a
                schedule:
                  duration: 10s
                timestamp:
                  event_time:
                    format: NOW
                    locale: en
                """);
        Path messageFile = flowDirectory.resolve("message.msg");
        Files.writeString(messageFile, "ts=${TIMESTAMP:event_time}");

        AppProperties appProperties = appProperties(dataDirectory);
        TemplateRendererService templateRendererService = new TemplateRendererService(new SimplePatternGenerator());
        FlowLoaderService flowLoaderService = new FlowLoaderService(appProperties, templateRendererService);
        TaskSchedulingService taskSchedulingService = Mockito.mock(TaskSchedulingService.class);
        @SuppressWarnings("unchecked")
        KafkaTemplate<String, String> kafkaTemplate = Mockito.mock(KafkaTemplate.class);

        LaunchService service = new LaunchService(
                flowLoaderService,
                templateRendererService,
                databusPayloadService(),
                kafkaTemplate,
                Clock.systemUTC(),
                taskSchedulingService,
                appProperties);
        try {
            service.initialize();

            verify(taskSchedulingService, timeout(5_000).atLeast(1)).scheduleAPeriodicTask(
                    eq("flow-a"),
                    any(TaskDefinitionBean.class),
                    eq(Duration.ofSeconds(10)));

            Files.writeString(messageFile, "ts=${TIMESTAMP:event_time},env=QA");

            verify(taskSchedulingService, timeout(5_000).atLeast(2)).scheduleAPeriodicTask(
                    eq("flow-a"),
                    any(TaskDefinitionBean.class),
                    eq(Duration.ofSeconds(10)));
        } finally {
            service.destroy();
        }
    }

    @Test
    void reloadFlowsShouldRemoveTasksForDeletedFlows() {
        FlowLoaderService flowLoaderService = Mockito.mock(FlowLoaderService.class);
        TaskSchedulingService taskSchedulingService = Mockito.mock(TaskSchedulingService.class);
        TemplateRendererService templateRendererService = new TemplateRendererService(new SimplePatternGenerator());
        @SuppressWarnings("unchecked")
        KafkaTemplate<String, String> kafkaTemplate = Mockito.mock(KafkaTemplate.class);

        FlowDefinition initialFlow = flowDefinition(
                "flow-a",
                "topic-a",
                durationSchedule("10s"),
                singleTimestampProfile("event_time", timestamp("NOW", null, "en")),
                Map.of(),
                List.of(Path.of("a.msg")));
        FlowDefinition remainingFlow = flowDefinition(
                "flow-b",
                "topic-b",
                durationSchedule("20s"),
                singleTimestampProfile("event_time", timestamp("NOW", null, "en")),
                Map.of(),
                List.of(Path.of("b.msg")));

        when(flowLoaderService.getAllFlows())
                .thenReturn(List.of(initialFlow, remainingFlow))
                .thenReturn(List.of(remainingFlow));

        LaunchService service = new LaunchService(
                flowLoaderService,
                templateRendererService,
                databusPayloadService(),
                kafkaTemplate,
                Clock.systemUTC(),
                taskSchedulingService,
                appProperties(tempDir));

        service.reloadFlows();
        service.reloadFlows();

        verify(taskSchedulingService).removeScheduledTask("flow-a");
        verify(taskSchedulingService, never()).removeScheduledTask("flow-b");
    }

    @Test
    void initializeShouldLoadFlowsOnlyOnceBeforeStartingWatcher() throws Exception {
        FlowLoaderService flowLoaderService = Mockito.mock(FlowLoaderService.class);
        TaskSchedulingService taskSchedulingService = Mockito.mock(TaskSchedulingService.class);
        TemplateRendererService templateRendererService = new TemplateRendererService(new SimplePatternGenerator());
        @SuppressWarnings("unchecked")
        KafkaTemplate<String, String> kafkaTemplate = Mockito.mock(KafkaTemplate.class);

        Files.createDirectories(tempDir.resolve("data"));

        FlowDefinition flowDefinition = flowDefinition(
                "flow-a",
                "topic-a",
                durationSchedule("10s"),
                singleTimestampProfile("event_time", timestamp("NOW", null, "en")),
                Map.of(),
                List.of(Path.of("a.msg")));

        when(flowLoaderService.getAllFlows()).thenReturn(List.of(flowDefinition));

        LaunchService service = new LaunchService(
                flowLoaderService,
                templateRendererService,
                databusPayloadService(),
                kafkaTemplate,
                Clock.systemUTC(),
                taskSchedulingService,
                appProperties(tempDir.resolve("data")));
        try {
            service.initialize();
        } finally {
            service.destroy();
        }

        verify(flowLoaderService, times(1)).getAllFlows();
    }

    private AppProperties appProperties(Path dataDirectory) {
        AppProperties appProperties = new AppProperties();
        appProperties.setDataDirectory(dataDirectory);
        return appProperties;
    }

    private DatabusPayloadService databusPayloadService() {
        return new DatabusPayloadService(new com.fasterxml.jackson.databind.ObjectMapper(), new AppProperties());
    }
}
