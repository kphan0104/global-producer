package com.global.producer.validator;

import com.global.producer.model.FlowDefinition;
import com.global.producer.model.Schedule;
import com.global.producer.model.TimestampDefinition;
import com.global.producer.model.VariableDefinition;
import java.util.Map;
import lombok.experimental.UtilityClass;
import org.springframework.util.StringUtils;

@UtilityClass
public class FlowDefinitionValidator {

    public void validate(FlowDefinition flowDefinition) {
        requireText(flowDefinition.getName(), "Flow name must not be blank");
        requireText(flowDefinition.getTopic(), "Flow " + flowDefinition.getName() + " must define a topic");
        validateTimestamp(flowDefinition);
        validateSchedule(flowDefinition);
        validateVariables(flowDefinition);

        if (flowDefinition.getMessageFiles() == null || flowDefinition.getMessageFiles().isEmpty()) {
            throw new IllegalArgumentException("Flow " + flowDefinition.getName() + " must contain at least one .msg file");
        }
    }

    private void validateSchedule(FlowDefinition flowDefinition) {
        Schedule schedule = flowDefinition.getSchedule();
        if (schedule == null || !schedule.isValid()) {
            throw new IllegalArgumentException(
                    "Flow " + flowDefinition.getName() + " must define exactly one schedule field: cron or duration");
        }
        if (schedule.hasDuration()) {
            schedule.resolveDuration();
            return;
        }

        if (!schedule.hasTimezone() && !canInferCronTimezone(flowDefinition)) {
            throw new IllegalArgumentException(
                    "Flow " + flowDefinition.getName()
                            + " must define schedule.timezone when cron timezone cannot be inferred from timestamp profiles");
        }
    }

    private void validateTimestamp(FlowDefinition flowDefinition) {
        Map<String, TimestampDefinition> timestampProfiles = flowDefinition.getTimestamp();
        if (timestampProfiles == null || timestampProfiles.isEmpty()) {
            throw new IllegalArgumentException("Flow " + flowDefinition.getName() + " must define timestamp settings");
        }

        for (Map.Entry<String, TimestampDefinition> entry : timestampProfiles.entrySet()) {
            String profileName = entry.getKey();
            TimestampDefinition timestampDefinition = entry.getValue();

            requireText(profileName, "Flow " + flowDefinition.getName() + " contains a blank timestamp profile name");
            if (timestampDefinition == null) {
                throw new IllegalArgumentException(
                        "Flow " + flowDefinition.getName() + " contains null config for timestamp profile " + profileName);
            }

            requireText(
                    timestampDefinition.getFormat(),
                    "Flow " + flowDefinition.getName() + " must define timestamp." + profileName + ".format");
            if (!"NOW".equalsIgnoreCase(timestampDefinition.getFormat())) {
                requireText(
                        timestampDefinition.getTimezone(),
                        "Flow " + flowDefinition.getName() + " must define timestamp." + profileName + ".timezone");
            }
        }
    }

    private void validateVariables(FlowDefinition flowDefinition) {
        if (flowDefinition.getVariables() == null) {
            flowDefinition.setVariables(Map.of());
            return;
        }

        for (Map.Entry<String, VariableDefinition> entry : flowDefinition.getVariables().entrySet()) {
            String variableName = entry.getKey();
            VariableDefinition variableDefinition = entry.getValue();

            requireText(variableName, "Flow " + flowDefinition.getName() + " contains a blank variable name");
            if ("TIMESTAMP".equals(variableName)) {
                throw new IllegalArgumentException("Flow " + flowDefinition.getName() + " must not redefine reserved variable TIMESTAMP");
            }
            if (variableDefinition == null) {
                throw new IllegalArgumentException("Flow " + flowDefinition.getName() + " contains null config for variable " + variableName);
            }

            boolean hasChoice = variableDefinition.getChoice() != null && !variableDefinition.getChoice().isEmpty();
            boolean hasPattern = StringUtils.hasText(variableDefinition.getPattern());

            if (hasChoice == hasPattern) {
                throw new IllegalArgumentException(
                        "Variable " + variableName + " in flow " + flowDefinition.getName()
                                + " must define exactly one of choice or pattern");
            }
            if (hasChoice && variableDefinition.getChoice().stream().anyMatch(choice -> !StringUtils.hasText(choice))) {
                throw new IllegalArgumentException(
                        "Variable " + variableName + " in flow " + flowDefinition.getName()
                                + " contains a blank choice value");
            }
        }
    }

    private void requireText(String value, String message) {
        if (!StringUtils.hasText(value)) {
            throw new IllegalArgumentException(message);
        }
    }

    private boolean canInferCronTimezone(FlowDefinition flowDefinition) {
        if (flowDefinition.getTimestamp().size() != 1) {
            return false;
        }

        TimestampDefinition onlyTimestampProfile = flowDefinition.getTimestamp().values().iterator().next();
        return !"NOW".equalsIgnoreCase(onlyTimestampProfile.getFormat())
                && StringUtils.hasText(onlyTimestampProfile.getTimezone());
    }
}
