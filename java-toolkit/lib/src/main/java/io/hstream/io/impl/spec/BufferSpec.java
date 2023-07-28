package io.hstream.io.impl.spec;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.BooleanNode;
import com.fasterxml.jackson.databind.node.IntNode;
import lombok.Getter;

import java.util.List;

public class BufferSpec implements SpecGroup {
    @Override
    public String name() {
        return "Buffer Strategy";
    }

    public List<SpecProperty> properties() {
        return List.of(new BatchMaxBytesSize(), new BatchMaxAge(), new EnableBackgroundFlush());
    }

    static public String BATCH_MAX_BYTES_SIZE = "buffer.batch.maxBytesSize";
    static public String BATCH_MAX_AGE = "buffer.batch.maxAge";
    static public String ENABLE_BACKGROUND_FLUSH = "buffer.enableBackgroundFlush";

    @Getter
    static public class BatchMaxBytesSize implements SpecProperty {
        String name = BATCH_MAX_BYTES_SIZE;
        String uiShowName = "Max Buffer Bytes for each Batch";
        String type = "integer";
        JsonNode defaultValue = new IntNode(0);
    }

    @Getter
    static public class BatchMaxAge implements SpecProperty {
        String name = BATCH_MAX_AGE;
        String uiShowName = "Max Batch Age(unit: milliseconds)";
        String type = "integer";
        JsonNode defaultValue = new IntNode(0);
    }

    @Getter
    static public class EnableBackgroundFlush implements SpecProperty {
        String name = ENABLE_BACKGROUND_FLUSH;
        String uiShowName = "Enable Background Flush";
        String type = "boolean";
        JsonNode defaultValue = BooleanNode.getFalse();
        List<JsonNode> enumValues = List.of(BooleanNode.getFalse(), BooleanNode.getTrue());
    }
}
