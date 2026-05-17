package com.global.producer.validator;

import static com.global.producer.support.FlowDefinitionTestFactory.choice;
import static com.global.producer.support.FlowDefinitionTestFactory.durationSchedule;
import static com.global.producer.support.FlowDefinitionTestFactory.flowDefinition;
import static com.global.producer.support.FlowDefinitionTestFactory.pattern;
import static com.global.producer.support.FlowDefinitionTestFactory.timestamp;
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
                timestamp("ISO_INSTANT", "UTC", "en"),
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
                timestamp("ISO_INSTANT", "UTC", "en"),
                Map.of(),
                java.util.List.of(Path.of("sample.msg")))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("exactly one schedule field");
    }

    @Test
    void validateShouldRejectReservedTimestampVariableName() {
        assertThatThrownBy(() -> FlowDefinitionValidator.validate(flowDefinition(
                "flow-invalid",
                "topic-invalid",
                durationSchedule("10s"),
                timestamp("ISO_INSTANT", "UTC", "en"),
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
                timestamp("ISO_INSTANT", "UTC", "en"),
                Map.of("CODE", invalidVariable),
                java.util.List.of(Path.of("sample.msg")))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("exactly one of choice or pattern");
    }
}

