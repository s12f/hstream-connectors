package io.hstream.io.impl.spec;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.SneakyThrows;

import java.util.LinkedList;

public class ExtendedSpec {
    @SneakyThrows
    static public String merge(JsonNode baseJson) {
        var properties = new LinkedList<SpecProperty>();
        properties.addAll(ErrorSpec.properties());
        properties.addAll(ReaderSpec.properties());
        properties.addAll(BufferSpec.properties());
        for (var p : properties) {
            if (p.getRequired()) {
                ((ArrayNode)baseJson.get("required")).add(p.getName());
            }
            ((ArrayNode)baseJson.get("ui:order")).add(p.getName());
            ((ObjectNode) baseJson.get("properties")).set(p.getName(), p.toProperty());
        }
        return baseJson.toString();
    }
}
