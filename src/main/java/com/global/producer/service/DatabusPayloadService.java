package com.global.producer.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.global.producer.model.FlowDefinition;
import com.global.producer.property.AppProperties;
import java.time.Instant;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class DatabusPayloadService {

    private static final String PIPELINE_ID = "global_producer";

    private final ObjectMapper objectMapper;
    private final AppProperties appProperties;

    public String wrap(FlowDefinition flowDefinition, String originalMessage, Instant now) {
        ObjectNode payload = objectMapper.createObjectNode();
        payload.put("databus.flow.name", flowDefinition.getName());
        payload.put("databus.flow.provider.name", appProperties.getDatabus().getFlow().getProvider().getName());
        payload.put("originalMessage", originalMessage);
        payload.put("databus.event.lineage.stage1.timestamp", now.toString());
        payload.put("databus.event.lineage.stage1.pipeline_id", PIPELINE_ID);
        try {
            return objectMapper.writeValueAsString(payload);
        } catch (JsonProcessingException exception) {
            throw new IllegalStateException("Unable to serialize Databus payload for flow " + flowDefinition.getName(), exception);
        }
    }
}
