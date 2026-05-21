package com.global.producer.service;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.global.producer.model.FlowDefinition;
import com.global.producer.property.AppProperties;
import java.time.Instant;
import org.junit.jupiter.api.Test;

class DatabusPayloadServiceTest {

    @Test
    void wrapShouldBuildExpectedDatabusEnvelope() throws Exception {
        AppProperties appProperties = new AppProperties();
        appProperties.getDatabus().getFlow().getProvider().setName("integration-tests");

        FlowDefinition flowDefinition = new FlowDefinition();
        flowDefinition.setName("flow-a");

        DatabusPayloadService service = new DatabusPayloadService(new ObjectMapper(), appProperties);

        String payload = service.wrap(
                flowDefinition,
                "{\"status\":\"OK\"}",
                Instant.parse("2026-05-21T13:00:00Z"));

        ObjectMapper objectMapper = new ObjectMapper();
        var json = objectMapper.readTree(payload);
        assertThat(json.get("databus.flow.name").asText()).isEqualTo("flow-a");
        assertThat(json.get("databus.flow.provider.name").asText()).isEqualTo("integration-tests");
        assertThat(json.get("originalMessage").asText()).isEqualTo("{\"status\":\"OK\"}");
        assertThat(json.get("databus.event.lineage.stage1.timestamp").asText()).isEqualTo("2026-05-21T13:00:00Z");
        assertThat(json.get("databus.event.lineage.stage1.pipeline_id").asText()).isEqualTo("global_producer");
    }
}
