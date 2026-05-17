package com.global.producer.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.time.Duration;
import java.util.concurrent.ScheduledFuture;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.Trigger;
import org.springframework.scheduling.support.CronTrigger;
import org.springframework.scheduling.support.PeriodicTrigger;

class TaskSchedulingServiceTest {

    @Test
    void scheduleACronTaskShouldUseCronTrigger() {
        TaskScheduler taskScheduler = mock(TaskScheduler.class);
        ScheduledFuture<?> future = mock(ScheduledFuture.class);
        doReturn(future).when(taskScheduler).schedule(any(Runnable.class), any(Trigger.class));

        TaskSchedulingService service = new TaskSchedulingService(taskScheduler);
        service.scheduleACronTask("flow-a", () -> { }, "0 */1 * * * *", "Europe/Paris");

        ArgumentCaptor<Trigger> triggerCaptor = ArgumentCaptor.forClass(Trigger.class);
        verify(taskScheduler).schedule(any(Runnable.class), triggerCaptor.capture());

        assertThat(triggerCaptor.getValue()).isInstanceOf(CronTrigger.class);
        assertThat(((CronTrigger) triggerCaptor.getValue()).getExpression()).isEqualTo("0 */1 * * * *");
    }

    @Test
    void scheduleAPeriodicTaskShouldUsePeriodicTrigger() {
        TaskScheduler taskScheduler = mock(TaskScheduler.class);
        ScheduledFuture<?> future = mock(ScheduledFuture.class);
        doReturn(future).when(taskScheduler).schedule(any(Runnable.class), any(Trigger.class));

        TaskSchedulingService service = new TaskSchedulingService(taskScheduler);
        service.scheduleAPeriodicTask("flow-a", () -> { }, Duration.ofSeconds(10));

        ArgumentCaptor<Trigger> triggerCaptor = ArgumentCaptor.forClass(Trigger.class);
        verify(taskScheduler).schedule(any(Runnable.class), triggerCaptor.capture());

        assertThat(triggerCaptor.getValue()).isInstanceOf(PeriodicTrigger.class);
        assertThat(((PeriodicTrigger) triggerCaptor.getValue()).getPeriodDuration()).isEqualTo(Duration.ofSeconds(10));
    }

    @Test
    void removeScheduledTaskShouldCancelExistingFuture() {
        TaskScheduler taskScheduler = mock(TaskScheduler.class);
        ScheduledFuture<?> future = mock(ScheduledFuture.class);
        doReturn(future).when(taskScheduler).schedule(any(Runnable.class), any(Trigger.class));

        TaskSchedulingService service = new TaskSchedulingService(taskScheduler);
        service.scheduleAPeriodicTask("flow-a", () -> { }, Duration.ofSeconds(10));

        service.removeScheduledTask("flow-a");

        verify(future).cancel(true);
    }

    @Test
    void reschedulingSameJobShouldCancelPreviousFuture() {
        TaskScheduler taskScheduler = mock(TaskScheduler.class);
        ScheduledFuture<?> firstFuture = mock(ScheduledFuture.class);
        ScheduledFuture<?> secondFuture = mock(ScheduledFuture.class);
        doReturn(firstFuture, secondFuture).when(taskScheduler).schedule(any(Runnable.class), any(Trigger.class));

        TaskSchedulingService service = new TaskSchedulingService(taskScheduler);
        service.scheduleAPeriodicTask("flow-a", () -> { }, Duration.ofSeconds(10));
        service.scheduleAPeriodicTask("flow-a", () -> { }, Duration.ofSeconds(20));

        verify(firstFuture).cancel(true);
    }

    @Test
    void scheduleAPeriodicTaskShouldFailWhenSchedulerReturnsNull() {
        TaskScheduler taskScheduler = mock(TaskScheduler.class);
        doReturn(null).when(taskScheduler).schedule(any(Runnable.class), any(Trigger.class));

        TaskSchedulingService service = new TaskSchedulingService(taskScheduler);

        assertThatThrownBy(() -> service.scheduleAPeriodicTask("flow-a", () -> { }, Duration.ofSeconds(10)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("flow-a");
    }
}
