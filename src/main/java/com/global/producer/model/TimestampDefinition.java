package com.global.producer.model;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class TimestampDefinition {

    private String format;
    private String timezone;
    private String locale;
}
