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
    private Databus databus = new Databus();

    @Getter
    @Setter
    public static class Databus {

        private Flow flow = new Flow();

        @Getter
        @Setter
        public static class Flow {

            private Provider provider = new Provider();

            @Getter
            @Setter
            public static class Provider {

                private String name = "integration-tests";
            }
        }
    }
}
