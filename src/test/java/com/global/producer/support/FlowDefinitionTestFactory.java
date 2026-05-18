package com.global.producer.support;

import com.global.producer.model.FlowDefinition;
import com.global.producer.model.Schedule;
import com.global.producer.model.TimestampDefinition;
import com.global.producer.model.VariableDefinition;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.Map;

public final class FlowDefinitionTestFactory {

    private FlowDefinitionTestFactory() {
    }

    public static FlowDefinition flowDefinition(
            String name,
            String topic,
            Schedule schedule,
            Map<String, TimestampDefinition> timestampProfiles,
            Map<String, VariableDefinition> variables,
            List<Path> messageFiles) {
        FlowDefinition flowDefinition = new FlowDefinition();
        flowDefinition.setName(name);
        flowDefinition.setTopic(topic);
        flowDefinition.setSchedule(schedule);
        flowDefinition.setTimestamp(new LinkedHashMap<>(timestampProfiles));
        flowDefinition.setVariables(new LinkedHashMap<>(variables));
        flowDefinition.setMessageFiles(messageFiles);
        return flowDefinition;
    }

    public static Schedule durationSchedule(String duration) {
        Schedule schedule = new Schedule();
        schedule.setDuration(duration);
        return schedule;
    }

    public static Schedule cronSchedule(String cron) {
        Schedule schedule = new Schedule();
        schedule.setCron(cron);
        return schedule;
    }

    public static Schedule cronSchedule(String cron, String timezone) {
        Schedule schedule = cronSchedule(cron);
        schedule.setTimezone(timezone);
        return schedule;
    }

    public static TimestampDefinition timestamp(String format, String timezone, String locale) {
        TimestampDefinition timestampDefinition = new TimestampDefinition();
        timestampDefinition.setFormat(format);
        timestampDefinition.setTimezone(timezone);
        timestampDefinition.setLocale(locale);
        return timestampDefinition;
    }

    public static Map<String, TimestampDefinition> singleTimestampProfile(String profileName, TimestampDefinition timestampDefinition) {
        return timestampProfiles(timestampProfile(profileName, timestampDefinition));
    }

    @SafeVarargs
    public static Map<String, TimestampDefinition> timestampProfiles(Entry<String, TimestampDefinition>... profiles) {
        Map<String, TimestampDefinition> timestampProfiles = new LinkedHashMap<>();
        for (Entry<String, TimestampDefinition> profile : profiles) {
            timestampProfiles.put(profile.getKey(), profile.getValue());
        }
        return timestampProfiles;
    }

    public static Entry<String, TimestampDefinition> timestampProfile(
            String profileName,
            TimestampDefinition timestampDefinition) {
        return Map.entry(profileName, timestampDefinition);
    }

    public static VariableDefinition choice(String... values) {
        VariableDefinition variableDefinition = new VariableDefinition();
        variableDefinition.setChoice(List.of(values));
        return variableDefinition;
    }

    public static VariableDefinition pattern(String regexSubset) {
        VariableDefinition variableDefinition = new VariableDefinition();
        variableDefinition.setPattern(regexSubset);
        return variableDefinition;
    }
}
