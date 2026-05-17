package com.global.producer.model;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class FlowDefinition {

    private String name;
    private Path directory;
    private String topic;
    private Schedule schedule;
    private TimestampDefinition timestamp;
    private Map<String, VariableDefinition> variables = new LinkedHashMap<>();
    private List<Path> messageFiles = new ArrayList<>();

}
