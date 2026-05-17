package com.global.producer.model;

import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class VariableDefinition {

    private List<String> choice = new ArrayList<>();
    private String pattern;

}
