package com.global.producer.service;

import java.time.Duration;
import java.time.ZoneId;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledFuture;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.support.CronTrigger;
import org.springframework.scheduling.support.PeriodicTrigger;
import org.springframework.stereotype.Service;

@Service
@Slf4j
@RequiredArgsConstructor
public class TaskSchedulingService {

    private final TaskScheduler taskScheduler;
    private final Map<String, ScheduledFuture<?>> jobsMap = new ConcurrentHashMap<>();

    public void scheduleACronTask(String jobId, Runnable taskLet, String cronExpression, String timezone) {
        log.info("Scheduling task with job id: {} and cron expression: {} in timezone {}", jobId, cronExpression, timezone);
        ScheduledFuture<?> scheduledTask = taskScheduler.schedule(
                taskLet,
                new CronTrigger(cronExpression, TimeZone.getTimeZone(ZoneId.of(timezone))));
        storeScheduledTask(jobId, scheduledTask);
    }

    public void scheduleAPeriodicTask(String jobId, Runnable taskLet, Duration duration) {
        log.info("Scheduling task with job id: {} and periodic duration: {}", jobId, duration);
        ScheduledFuture<?> scheduledTask = taskScheduler.schedule(
                taskLet,
                new PeriodicTrigger(duration));
        storeScheduledTask(jobId, scheduledTask);
    }

    public void removeScheduledTask(String jobId) {
        ScheduledFuture<?> scheduledTask = jobsMap.get(jobId);
        if (scheduledTask != null) {
            scheduledTask.cancel(true);
            jobsMap.remove(jobId);
        }
    }

    private void storeScheduledTask(String jobId, ScheduledFuture<?> scheduledTask) {
        if (scheduledTask == null) {
            throw new IllegalStateException("Task scheduler returned null for job " + jobId);
        }

        ScheduledFuture<?> previousTask = jobsMap.put(jobId, scheduledTask);
        if (previousTask != null) {
            previousTask.cancel(true);
            log.info("Replaced existing scheduled task with job id: {}", jobId);
        }
    }
}
