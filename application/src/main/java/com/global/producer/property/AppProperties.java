package com.global.producer.property;

import java.nio.file.Path;
import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "app")
@Getter
@Setter
public class AppProperties {

    private Path dataDirectory = Path.of("data");
}
