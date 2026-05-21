package com.global.producer.service;

import com.global.producer.model.FlowDefinition;
import com.global.producer.property.AppProperties;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.ClosedWatchServiceException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.time.Clock;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class LaunchService {

    private final FlowLoaderService flowLoaderService;
    private final TemplateRendererService templateRendererService;
    private final DatabusPayloadService databusPayloadService;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final Clock clock;
    private final TaskSchedulingService taskSchedulingService;
    private final Path dataDirectory;
    private final Object monitor = new Object();
    private final Set<String> scheduledFlowNames = new HashSet<>();
    private final Map<WatchKey, Path> watchDirectoriesByKey = new HashMap<>();

    private WatchService watchService;
    private Thread watchThread;

    public LaunchService(
            FlowLoaderService flowLoaderService,
            TemplateRendererService templateRendererService,
            DatabusPayloadService databusPayloadService,
            KafkaTemplate<String, String> kafkaTemplate,
            Clock clock,
            TaskSchedulingService taskSchedulingService,
            AppProperties appProperties) {
        this.flowLoaderService = flowLoaderService;
        this.templateRendererService = templateRendererService;
        this.databusPayloadService = databusPayloadService;
        this.kafkaTemplate = kafkaTemplate;
        this.clock = clock;
        this.taskSchedulingService = taskSchedulingService;
        this.dataDirectory = appProperties.getDataDirectory().toAbsolutePath().normalize();
    }

    @PostConstruct
    public void initialize() {
        List<FlowDefinition> flowDefinitions = flowLoaderService.getAllFlows();
        synchronized (monitor) {
            scheduleFlows(flowDefinitions);
        }
        startWatchingDataDirectory(flowDefinitions);
    }

    @PreDestroy
    public void destroy() {
        stopWatchingDataDirectory();
        synchronized (monitor) {
            scheduledFlowNames.forEach(taskSchedulingService::removeScheduledTask);
            scheduledFlowNames.clear();
        }
    }

    void reloadFlows() {
        List<FlowDefinition> flowDefinitions = flowLoaderService.getAllFlows();
        synchronized (monitor) {
            scheduleFlows(flowDefinitions);
            refreshWatchRegistrations(flowDefinitions);
        }
    }

    private void scheduleFlows(List<FlowDefinition> flowDefinitions) {
        Set<String> reloadedFlowNames = new HashSet<>();

        flowDefinitions.forEach(flowDefinition -> {
            TaskDefinitionBean taskDefinitionBean = new TaskDefinitionBean(
                    flowDefinition,
                    templateRendererService,
                    databusPayloadService,
                    kafkaTemplate,
                    clock);

            var schedule = flowDefinition.getSchedule();
            if (schedule.hasDuration()) {
                taskSchedulingService.scheduleAPeriodicTask(
                        flowDefinition.getName(),
                        taskDefinitionBean,
                        schedule.resolveDuration());
            } else {
                taskSchedulingService.scheduleACronTask(
                        flowDefinition.getName(),
                        taskDefinitionBean,
                        schedule.getCron(),
                        resolveCronTimezone(flowDefinition));
            }

            reloadedFlowNames.add(flowDefinition.getName());
        });

        new HashSet<>(scheduledFlowNames).stream()
                .filter(existingFlowName -> !reloadedFlowNames.contains(existingFlowName))
                .forEach(taskSchedulingService::removeScheduledTask);

        scheduledFlowNames.clear();
        scheduledFlowNames.addAll(reloadedFlowNames);
    }

    private String resolveCronTimezone(FlowDefinition flowDefinition) {
        if (flowDefinition.getSchedule().hasTimezone()) {
            return flowDefinition.getSchedule().getTimezone();
        }

        String inferredTimezone = flowDefinition.getTimestamp().values().iterator().next().getTimezone();
        if (inferredTimezone == null) {
            throw new IllegalStateException(
                    "Unable to infer cron timezone for flow " + flowDefinition.getName() + "; configure schedule.timezone");
        }
        return inferredTimezone;
    }

    private void startWatchingDataDirectory(List<FlowDefinition> flowDefinitions) {
        try {
            watchService = FileSystems.getDefault().newWatchService();
            synchronized (monitor) {
                refreshWatchRegistrations(flowDefinitions);
            }
            watchThread = Thread.ofPlatform()
                    .daemon(true)
                    .name("flow-data-watcher")
                    .start(this::watchLoop);
        } catch (IOException exception) {
            throw new UncheckedIOException("Unable to create watch service for " + dataDirectory, exception);
        }
    }

    private void stopWatchingDataDirectory() {
        if (watchService != null) {
            try {
                watchService.close();
            } catch (IOException exception) {
                log.warn("Failed to close watch service for {}", dataDirectory, exception);
            }
        }

        if (watchThread != null) {
            watchThread.interrupt();
            try {
                watchThread.join(1_000L);
            } catch (InterruptedException exception) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private void watchLoop() {
        while (!Thread.currentThread().isInterrupted()) {
            WatchKey watchKey;
            try {
                watchKey = watchService.take();
            } catch (InterruptedException exception) {
                Thread.currentThread().interrupt();
                return;
            } catch (ClosedWatchServiceException exception) {
                return;
            }

            Path watchedDirectory;
            synchronized (monitor) {
                watchedDirectory = watchDirectoriesByKey.get(watchKey);
            }
            if (watchedDirectory == null) {
                watchKey.reset();
                continue;
            }

            boolean reloadRequired = false;
            for (WatchEvent<?> event : watchKey.pollEvents()) {
                WatchEvent.Kind<?> kind = event.kind();
                if (kind == StandardWatchEventKinds.OVERFLOW) {
                    reloadRequired = true;
                    continue;
                }

                Path changedPath = watchedDirectory.resolve((Path) event.context());
                if (watchedDirectory.equals(dataDirectory)
                        && kind == StandardWatchEventKinds.ENTRY_CREATE
                        && Files.isDirectory(changedPath)) {
                    synchronized (monitor) {
                        registerWatchDirectory(changedPath);
                    }
                }

                if (shouldReload(watchedDirectory, changedPath)) {
                    reloadRequired = true;
                }
            }

            if (!watchKey.reset()) {
                synchronized (monitor) {
                    watchDirectoriesByKey.remove(watchKey);
                }
            }

            if (reloadRequired) {
                reloadFlowsSafely();
            }
        }
    }

    private void reloadFlowsSafely() {
        try {
            reloadFlows();
            log.info("Reloaded flow definitions from {}", dataDirectory);
        } catch (RuntimeException exception) {
            log.error("Failed to reload flow definitions from {}", dataDirectory, exception);
        }
    }

    private boolean shouldReload(Path watchedDirectory, Path changedPath) {
        if (watchedDirectory.equals(dataDirectory)) {
            return true;
        }

        String fileName = changedPath.getFileName().toString();
        return fileName.endsWith(".msg") || fileName.endsWith(".yml") || fileName.endsWith(".yaml");
    }

    private void refreshWatchRegistrations(List<FlowDefinition> flowDefinitions) {
        if (watchService == null) {
            return;
        }

        watchDirectoriesByKey.keySet().forEach(WatchKey::cancel);
        watchDirectoriesByKey.clear();

        registerWatchDirectory(dataDirectory);
        flowDefinitions.stream()
                .map(FlowDefinition::getDirectory)
                .filter(Objects::nonNull)
                .distinct()
                .forEach(this::registerWatchDirectory);
    }

    private void registerWatchDirectory(Path directory) {
        if (!Files.isDirectory(directory)) {
            return;
        }

        try {
            WatchKey watchKey = directory.register(
                    watchService,
                    StandardWatchEventKinds.ENTRY_CREATE,
                    StandardWatchEventKinds.ENTRY_DELETE,
                    StandardWatchEventKinds.ENTRY_MODIFY);
            watchDirectoriesByKey.put(watchKey, directory);
        } catch (IOException exception) {
            throw new UncheckedIOException("Unable to watch directory " + directory, exception);
        }
    }
}
