package com.global.producer.configuration;

import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Contact;
import io.swagger.v3.oas.models.info.Info;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class OpenApiConfiguration {

    @Bean
    public OpenAPI globalProducerOpenApi() {
        return new OpenAPI().info(new Info()
                .title("Global Producer API")
                .description("Endpoints to publish direct Kafka messages outside the scheduled flow engine.")
                .version("1.0.0")
                .contact(new Contact().name("global-producer")));
    }
}
