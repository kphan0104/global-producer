package com.global.producer.service;

import com.global.producer.model.FlowDefinition;
import com.global.producer.model.TimestampDefinition;
import com.global.producer.model.VariableDefinition;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.WeakHashMap;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

@Service
@RequiredArgsConstructor
public class TemplateRendererService {

    private static final Pattern PLACEHOLDER_PATTERN = Pattern.compile("\\$\\{([A-Za-z0-9_]+)(?::([A-Za-z0-9_]+))?}");
    private static final String TIMESTAMP_PLACEHOLDER = "TIMESTAMP";

    private final SimplePatternGenerator simplePatternGenerator;
    private final Map<FlowDefinition, CompiledFlowDefinition> compiledFlows =
            Collections.synchronizedMap(new WeakHashMap<>());

    public void validateFlowDefinition(FlowDefinition flowDefinition) {
        compiledFlows.put(flowDefinition, compileFlowDefinition(flowDefinition));
    }

    public String render(FlowDefinition flowDefinition, Path messageFile, Instant now) {
        CompiledFlowDefinition compiledFlowDefinition = compiledFlowDefinition(flowDefinition);
        CompiledTemplate compiledTemplate = compiledFlowDefinition.compiledTemplates().get(messageFile.normalize());
        if (compiledTemplate == null) {
            throw new IllegalArgumentException(
                    "Flow " + flowDefinition.getName() + " does not contain compiled template for " + messageFile);
        }

        Map<String, String> renderedTimestamps = new HashMap<>();
        Map<String, String> variableValues = new HashMap<>();
        StringBuilder renderedContent = new StringBuilder(compiledTemplate.templateLengthHint());
        for (CompiledTemplatePart templatePart : compiledTemplate.parts()) {
            if (templatePart instanceof LiteralPart literalPart) {
                renderedContent.append(literalPart.content());
                continue;
            }
            if (templatePart instanceof TimestampPlaceholderPart timestampPlaceholderPart) {
                renderedContent.append(renderedTimestamps.computeIfAbsent(
                        timestampPlaceholderPart.profileName(),
                        ignored -> timestampPlaceholderPart.formatter().format(now)));
                continue;
            }

            VariablePlaceholderPart placeholderPart = (VariablePlaceholderPart) templatePart;
            renderedContent.append(variableValues.computeIfAbsent(
                    placeholderPart.name(),
                    ignored -> resolveVariableValue(placeholderPart.variableDefinition())));
        }

        return renderedContent.toString();
    }

    private CompiledFlowDefinition compileFlowDefinition(FlowDefinition flowDefinition) {
        Map<String, DateTimeFormatter> timestampFormatters = compileTimestampFormatters(flowDefinition);
        flowDefinition.getVariables().forEach((variableName, variableDefinition) -> {
            if (hasPattern(variableDefinition)) {
                simplePatternGenerator.validate(variableDefinition.getPattern());
            }
        });

        Map<Path, CompiledTemplate> compiledTemplates = new LinkedHashMap<>();
        flowDefinition.getMessageFiles().forEach(messageFile -> {
            String template = readTemplate(messageFile);
            CompiledTemplate compiledTemplate =
                    compileTemplate(flowDefinition, messageFile, template, timestampFormatters);
            compiledTemplates.put(messageFile.normalize(), compiledTemplate);
        });

        return new CompiledFlowDefinition(Map.copyOf(timestampFormatters), Map.copyOf(compiledTemplates));
    }

    private CompiledFlowDefinition compiledFlowDefinition(FlowDefinition flowDefinition) {
        CompiledFlowDefinition compiledFlowDefinition = compiledFlows.get(flowDefinition);
        if (compiledFlowDefinition != null) {
            return compiledFlowDefinition;
        }

        CompiledFlowDefinition newlyCompiledFlowDefinition = compileFlowDefinition(flowDefinition);
        compiledFlows.put(flowDefinition, newlyCompiledFlowDefinition);
        return newlyCompiledFlowDefinition;
    }

    private String resolveVariableValue(VariableDefinition variableDefinition) {
        if (hasChoices(variableDefinition)) {
            List<String> choices = variableDefinition.getChoice();
            return choices.get(ThreadLocalRandom.current().nextInt(choices.size()));
        }
        if (hasPattern(variableDefinition)) {
            return simplePatternGenerator.generate(variableDefinition.getPattern(), ThreadLocalRandom.current());
        }
        throw new IllegalArgumentException("Variable definition must define either choice or pattern");
    }

    private Map<String, DateTimeFormatter> compileTimestampFormatters(FlowDefinition flowDefinition) {
        Map<String, DateTimeFormatter> timestampFormatters = new LinkedHashMap<>();
        flowDefinition.getTimestamp().forEach((profileName, timestampDefinition) ->
                timestampFormatters.put(profileName, timestampFormatter(timestampDefinition)));
        return timestampFormatters;
    }

    private CompiledTemplate compileTemplate(
            FlowDefinition flowDefinition,
            Path messageFile,
            String template,
            Map<String, DateTimeFormatter> timestampFormatters) {
        List<CompiledTemplatePart> parts = new ArrayList<>();
        Matcher matcher = PLACEHOLDER_PATTERN.matcher(template);
        int currentIndex = 0;

        while (matcher.find()) {
            if (matcher.start() > currentIndex) {
                parts.add(new LiteralPart(template.substring(currentIndex, matcher.start())));
            }

            parts.add(resolvePlaceholderPart(flowDefinition, messageFile, matcher.group(1), matcher.group(2), timestampFormatters));
            currentIndex = matcher.end();
        }

        if (currentIndex < template.length()) {
            parts.add(new LiteralPart(template.substring(currentIndex)));
        }

        if (parts.isEmpty()) {
            parts.add(new LiteralPart(template));
        }

        return new CompiledTemplate(List.copyOf(parts), template.length());
    }

    private CompiledTemplatePart resolvePlaceholderPart(
            FlowDefinition flowDefinition,
            Path messageFile,
            String placeholder,
            String timestampProfile,
            Map<String, DateTimeFormatter> timestampFormatters) {
        if (TIMESTAMP_PLACEHOLDER.equals(placeholder)) {
            if (!StringUtils.hasText(timestampProfile)) {
                if (timestampFormatters.size() == 1) {
                    String singleProfileName = timestampFormatters.keySet().iterator().next();
                    return new TimestampPlaceholderPart(singleProfileName, timestampFormatters.get(singleProfileName));
                }

                throw new IllegalArgumentException(
                        "Flow " + flowDefinition.getName()
                                + " uses ${TIMESTAMP} in " + messageFile.getFileName()
                                + " but defines multiple timestamp profiles; use ${TIMESTAMP:<profile>}");
            }

            DateTimeFormatter formatter = timestampFormatters.get(timestampProfile);
            if (formatter == null) {
                throw new IllegalArgumentException(
                        "Flow " + flowDefinition.getName()
                                + " uses unknown timestamp profile " + timestampProfile
                                + " in " + messageFile.getFileName());
            }

            return new TimestampPlaceholderPart(timestampProfile, formatter);
        }

        if (StringUtils.hasText(timestampProfile)) {
            throw new IllegalArgumentException(
                    "Flow " + flowDefinition.getName()
                            + " uses profiled placeholder ${" + placeholder + ":" + timestampProfile + "} in "
                            + messageFile.getFileName()
                            + ", but only TIMESTAMP placeholders support profiles");
        }

        VariableDefinition variableDefinition = flowDefinition.getVariables().get(placeholder);
        if (variableDefinition == null) {
            throw new IllegalArgumentException(
                    "Flow " + flowDefinition.getName()
                            + " uses placeholder ${" + placeholder + "} in " + messageFile.getFileName()
                            + " without a matching variables entry");
        }

        return new VariablePlaceholderPart(placeholder, variableDefinition);
    }

    private String readTemplate(Path messageFile) {
        try {
            return Files.readString(messageFile);
        } catch (IOException exception) {
            throw new UncheckedIOException(exception);
        }
    }

    private DateTimeFormatter timestampFormatter(TimestampDefinition timestampDefinition) {
        Locale locale = resolveLocale(timestampDefinition.getLocale());
        String format = timestampDefinition.getFormat();

        if ("NOW".equalsIgnoreCase(format)) {
            return DateTimeFormatter.ISO_INSTANT.withLocale(locale).withZone(ZoneOffset.UTC);
        }

        ZoneId zoneId = resolveZoneId(timestampDefinition.getTimezone());
        if ("ISO_OFFSET_DATE_TIME".equalsIgnoreCase(format)) {
            return DateTimeFormatter.ISO_OFFSET_DATE_TIME.withLocale(locale).withZone(zoneId);
        }

        return DateTimeFormatter.ofPattern(format, locale).withZone(zoneId);
    }

    private Locale resolveLocale(String localeValue) {
        if (!StringUtils.hasText(localeValue)) {
            return Locale.ENGLISH;
        }
        return Locale.forLanguageTag(localeValue.replace('_', '-'));
    }

    private ZoneId resolveZoneId(String timezoneValue) {
        if (!StringUtils.hasText(timezoneValue)) {
            throw new IllegalArgumentException("timestamp.timezone must not be blank");
        }
        if (timezoneValue.startsWith("+") || timezoneValue.startsWith("-")) {
            return ZoneOffset.of(timezoneValue);
        }
        return ZoneId.of(timezoneValue);
    }

    private boolean hasChoices(VariableDefinition variableDefinition) {
        return variableDefinition.getChoice() != null && !variableDefinition.getChoice().isEmpty();
    }

    private boolean hasPattern(VariableDefinition variableDefinition) {
        return StringUtils.hasText(variableDefinition.getPattern());
    }

    private record CompiledFlowDefinition(
            Map<String, DateTimeFormatter> timestampFormatters,
            Map<Path, CompiledTemplate> compiledTemplates) {
    }

    private record CompiledTemplate(
            List<CompiledTemplatePart> parts,
            int templateLengthHint) {
    }

    private sealed interface CompiledTemplatePart permits LiteralPart, TimestampPlaceholderPart, VariablePlaceholderPart {
    }

    private record LiteralPart(String content) implements CompiledTemplatePart {
    }

    private record TimestampPlaceholderPart(String profileName, DateTimeFormatter formatter)
            implements CompiledTemplatePart {
    }

    private record VariablePlaceholderPart(String name, VariableDefinition variableDefinition)
            implements CompiledTemplatePart {
    }
}
