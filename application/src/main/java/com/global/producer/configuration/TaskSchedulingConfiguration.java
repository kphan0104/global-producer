package com.global.producer.configuration;

import com.global.producer.service.FlowLoaderService;
import com.global.producer.service.TaskDefinitionBean;
import com.global.producer.service.TemplateRendererService;
import java.time.Clock;
import java.util.List;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;

@Configuration
public class TaskSchedulingConfiguration {

    @Bean
    public List<TaskDefinitionBean> taskDefinitionBeans(
            FlowLoaderService flowLoaderService,
            TemplateRendererService templateRendererService,
            KafkaTemplate<String, String> kafkaTemplate,
            Clock clock) {
        return flowLoaderService.getAllFlows().stream()
                .map(flowDefinition -> new TaskDefinitionBean(flowDefinition, templateRendererService, kafkaTemplate, clock))
                .toList();
    }
}
