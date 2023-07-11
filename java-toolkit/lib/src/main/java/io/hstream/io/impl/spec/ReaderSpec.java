package io.hstream.io.impl.spec;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.TextNode;
import io.hstream.StreamShardOffset;
import io.hstream.io.Utils;
import lombok.Getter;

import java.util.List;

import static io.hstream.StreamShardOffset.SpecialOffset.EARLIEST;
import static io.hstream.StreamShardOffset.SpecialOffset.LATEST;

public class ReaderSpec {
    static List<SpecProperty> properties() {
        return List.of(new FromOffsetType(), new SpecialOffset());
    }

    static public String FROM_OFFSET_TYPE = "task.reader.fromOffsetType";
    static public String SPECIAL_OFFSET_NAME = "task.reader.specialOffset";

    @Getter
    static public class FromOffsetType implements SpecProperty {
        String name = FROM_OFFSET_TYPE;
        String uiShowName = "From Offset Type";
        String type = "string";
        List<JsonNode> enumValues = List.of(
                new TextNode(StreamShardOffset.OffsetType.SPECIAL.name())
            );
        JsonNode defaultValue = new TextNode(StreamShardOffset.OffsetType.SPECIAL.name());
    }

    @Getter
    static public class SpecialOffset implements SpecProperty {
        String name = SPECIAL_OFFSET_NAME;
        String uiShowName = "Special From Offset";
        JsonNode uiCondition = Utils.mapper.createObjectNode()
                .put("field", FROM_OFFSET_TYPE)
                .put("value", StreamShardOffset.OffsetType.SPECIAL.name());
        List<JsonNode> enumValues = List.of(new TextNode(EARLIEST.name()), new TextNode(LATEST.name()));
        String type = "string";
        JsonNode defaultValue = new TextNode(EARLIEST.toString());
    }
}
