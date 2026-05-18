package com.global.producer.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.global.producer.model.FlowDefinition;
import com.global.producer.property.AppProperties;
import com.global.producer.validator.FlowDefinitionValidator;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Stream;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Service
@Slf4j
@RequiredArgsConstructor
public class FlowLoaderService {

    private static final ObjectMapper YAML_MAPPER = new ObjectMapper(new YAMLFactory()).findAndRegisterModules();

    private final AppProperties appProperties;
    private final TemplateRendererService templateRendererService;

    public List<FlowDefinition> getAllFlows() {
        Path dataDirectory = appProperties.getDataDirectory().toAbsolutePath().normalize();

        if (!Files.isDirectory(dataDirectory)) {
            throw new IllegalArgumentException("Unable to find data directory: " + dataDirectory);
        }

        try (Stream<Path> directoryStream = Files.list(dataDirectory)) {
            List<FlowDefinition> flows = directoryStream
                    .filter(Files::isDirectory)
                    .sorted(Comparator.comparing(path -> path.getFileName().toString()))
                    .map(this::loadFlow)
                    .toList();

            if (flows.isEmpty()) {
                throw new IllegalArgumentException("No flow folders found in data directory: " + dataDirectory);
            }

            return flows;
        } catch (IOException exception) {
            throw new UncheckedIOException(exception);
        }
    }

    private FlowDefinition loadFlow(Path flowDirectory) {
        String flowName = flowDirectory.getFileName().toString();
        Path flowConfigurationFile = resolveConfigurationFile(flowDirectory, flowName);
        FlowDefinition flowDefinition = readFlowDefinition(flowConfigurationFile);

        flowDefinition.setName(flowName);
        flowDefinition.setDirectory(flowDirectory.toAbsolutePath().normalize());
        flowDefinition.setMessageFiles(loadMessageFiles(flowDirectory));

        FlowDefinitionValidator.validate(flowDefinition);
        templateRendererService.validateFlowDefinition(flowDefinition);

        log.info(
                "Loaded flow {} with {} message template(s) from {}",
                flowName,
                flowDefinition.getMessageFiles().size(),
                flowDirectory.toAbsolutePath().normalize());

        return flowDefinition;
    }

    private Path resolveConfigurationFile(Path flowDirectory, String flowName) {
        Path ymlFile = flowDirectory.resolve(flowName + ".yml");
        Path yamlFile = flowDirectory.resolve(flowName + ".yaml");

        boolean hasYml = Files.isRegularFile(ymlFile);
        boolean hasYaml = Files.isRegularFile(yamlFile);

        if (hasYml && hasYaml) {
            throw new IllegalArgumentException("Flow " + flowName + " has both .yml and .yaml configuration files");
        }
        if (hasYml) {
            return ymlFile;
        }
        if (hasYaml) {
            return yamlFile;
        }

        throw new IllegalArgumentException(
                "Unable to find flow configuration file " + flowName + ".yml or " + flowName + ".yaml in "
                        + flowDirectory.toAbsolutePath().normalize());
    }

    private FlowDefinition readFlowDefinition(Path flowConfigurationFile) {
        try {
            return YAML_MAPPER.readValue(flowConfigurationFile.toFile(), FlowDefinition.class);
        } catch (IOException exception) {
            throw new UncheckedIOException(exception);
        }
    }

    private List<Path> loadMessageFiles(Path flowDirectory) {
        try (Stream<Path> fileStream = Files.list(flowDirectory)) {
            return fileStream
                    .filter(Files::isRegularFile)
                    .filter(path -> path.getFileName().toString().endsWith(".msg"))
                    .sorted(Comparator.comparing(path -> path.getFileName().toString()))
                    .map(path -> path.toAbsolutePath().normalize())
                    .toList();
        } catch (IOException exception) {
            throw new UncheckedIOException(exception);
        }
    }
}
