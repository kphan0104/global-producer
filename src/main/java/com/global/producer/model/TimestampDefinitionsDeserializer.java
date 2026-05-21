package com.global.producer.model;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.ObjectCodec;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

public class TimestampDefinitionsDeserializer extends JsonDeserializer<Map<String, TimestampDefinition>> {

    @Override
    public Map<String, TimestampDefinition> deserialize(JsonParser parser, DeserializationContext context)
            throws IOException {
        ObjectCodec codec = parser.getCodec();
        JsonNode node = codec.readTree(parser);
        if (node == null || node.isNull()) {
            return new LinkedHashMap<>();
        }
        if (!node.isObject()) {
            throw JsonMappingException.from(parser, "timestamp must be an object");
        }

        if (looksLikeSingleTimestampDefinition(node)) {
            return new LinkedHashMap<>(Map.of(
                    FlowDefinition.DEFAULT_TIMESTAMP_PROFILE,
                    codec.treeToValue(node, TimestampDefinition.class)));
        }

        Map<String, TimestampDefinition> timestampProfiles = new LinkedHashMap<>();
        Iterator<Map.Entry<String, JsonNode>> fields = node.fields();
        while (fields.hasNext()) {
            Map.Entry<String, JsonNode> field = fields.next();
            timestampProfiles.put(field.getKey(), codec.treeToValue(field.getValue(), TimestampDefinition.class));
        }
        return timestampProfiles;
    }

    private boolean looksLikeSingleTimestampDefinition(JsonNode node) {
        return node.has("format") || node.has("timezone") || node.has("locale");
    }
}
