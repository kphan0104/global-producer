package com.global.producer.validator;

import static com.global.producer.support.FlowDefinitionTestFactory.choice;
import static com.global.producer.support.FlowDefinitionTestFactory.cronSchedule;
import static com.global.producer.support.FlowDefinitionTestFactory.durationSchedule;
import static com.global.producer.support.FlowDefinitionTestFactory.flowDefinition;
import static com.global.producer.support.FlowDefinitionTestFactory.pattern;
import static com.global.producer.support.FlowDefinitionTestFactory.singleTimestampProfile;
import static com.global.producer.support.FlowDefinitionTestFactory.timestamp;
import static com.global.producer.support.FlowDefinitionTestFactory.timestampProfile;
import static com.global.producer.support.FlowDefinitionTestFactory.timestampProfiles;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.global.producer.model.Schedule;
import java.nio.file.Path;
import java.util.Map;
import org.junit.jupiter.api.Test;

class FlowDefinitionValidatorTest {

    @Test
    void validateShouldAcceptValidFlowDefinition() {
        assertThatCode(() -> FlowDefinitionValidator.validate(flowDefinition(
                "flow-valid",
                "topic-valid",
                durationSchedule("10s"),
                singleTimestampProfile("event_time", timestamp("NOW", null, "en")),
                Map.of("ENV", choice("DEV"), "CODE", pattern("[A-Z]{3}[0-9]{2}")),
                java.util.List.of(Path.of("sample.msg")))))
                .doesNotThrowAnyException();
    }

    @Test
    void validateShouldRejectScheduleWithCronAndDuration() {
        Schedule schedule = new Schedule();
        schedule.setCron("0 */1 * * * *");
        schedule.setDuration("10s");

        assertThatThrownBy(() -> FlowDefinitionValidator.validate(flowDefinition(
                "flow-invalid",
                "topic-invalid",
                schedule,
                singleTimestampProfile("event_time", timestamp("NOW", null, "en")),
                Map.of(),
                java.util.List.of(Path.of("sample.msg")))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("exactly one schedule field");
    }

    @Test
    void validateShouldRejectCronWithoutScheduleTimezoneWhenMultipleTimestampProfilesExist() {
        assertThatThrownBy(() -> FlowDefinitionValidator.validate(flowDefinition(
                "flow-invalid",
                "topic-invalid",
                cronSchedule("0 */1 * * * *"),
                timestampProfiles(
                        timestampProfile("event_time", timestamp("NOW", null, "en")),
                        timestampProfile("local_time", timestamp("yyyy-MM-dd HH:mm:ss", "Europe/Paris", "fr"))),
                Map.of(),
                java.util.List.of(Path.of("sample.msg")))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("schedule.timezone");
    }

    @Test
    void validateShouldAllowCronWithoutScheduleTimezoneWhenSingleTimestampProfileHasTimezone() {
        assertThatCode(() -> FlowDefinitionValidator.validate(flowDefinition(
                "flow-valid",
                "topic-valid",
                cronSchedule("0 */1 * * * *"),
                singleTimestampProfile("event_time", timestamp("yyyy-MM-dd HH:mm:ss", "Europe/Paris", "fr")),
                Map.of(),
                java.util.List.of(Path.of("sample.msg")))))
                .doesNotThrowAnyException();
    }

    @Test
    void validateShouldRejectCronWithoutScheduleTimezoneWhenSingleTimestampProfileUsesNow() {
        assertThatThrownBy(() -> FlowDefinitionValidator.validate(flowDefinition(
                "flow-invalid",
                "topic-invalid",
                cronSchedule("0 */1 * * * *"),
                singleTimestampProfile("event_time", timestamp("NOW", "Europe/Paris", "en")),
                Map.of(),
                java.util.List.of(Path.of("sample.msg")))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("schedule.timezone");
    }

    @Test
    void validateShouldRejectReservedTimestampVariableName() {
        assertThatThrownBy(() -> FlowDefinitionValidator.validate(flowDefinition(
                "flow-invalid",
                "topic-invalid",
                durationSchedule("10s"),
                singleTimestampProfile("event_time", timestamp("NOW", null, "en")),
                Map.of("TIMESTAMP", choice("DEV")),
                java.util.List.of(Path.of("sample.msg")))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must not redefine reserved variable TIMESTAMP");
    }

    @Test
    void validateShouldRejectVariableWithChoiceAndPattern() {
        var invalidVariable = pattern("[A-Z]{3}");
        invalidVariable.setChoice(java.util.List.of("A"));

        assertThatThrownBy(() -> FlowDefinitionValidator.validate(flowDefinition(
                "flow-invalid",
                "topic-invalid",
                durationSchedule("10s"),
                singleTimestampProfile("event_time", timestamp("NOW", null, "en")),
                Map.of("CODE", invalidVariable),
                java.util.List.of(Path.of("sample.msg")))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("exactly one of choice or pattern");
    }
}
