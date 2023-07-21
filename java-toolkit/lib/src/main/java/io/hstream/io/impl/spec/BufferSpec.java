package io.hstream.io.impl.spec;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.BooleanNode;
import com.fasterxml.jackson.databind.node.IntNode;
import io.hstream.io.Utils;
import lombok.Getter;

import java.util.List;

public class BufferSpec {
    static List<SpecProperty> properties() {
        return List.of(new BatchMaxBytesSize(), new BatchMaxAge(), new EnableBackgroundFlush());
    }

    static public String BATCH_MAX_BYTES_SIZE = "buffer.batch.maxBytesSize";
    static public String BATCH_MAX_AGE = "buffer.batch.maxAge";
    static public String ENABLE_BACKGROUND_FLUSH = "buffer.enableBackgroundFlush";

    @Getter
    static public abstract class BufferProperty implements SpecProperty {
        String uiGroup = "buffer";
    }

    @Getter
    static public class BatchMaxBytesSize extends BufferProperty {
        String name = BATCH_MAX_BYTES_SIZE;
        String uiShowName = "Max Buffer Bytes for each Batch";
        String type = "integer";
        JsonNode defaultValue = new IntNode(1048576);
    }

    @Getter
    static public class BatchMaxAge extends BufferProperty {
        String name = BATCH_MAX_AGE;
        String uiShowName = "Max Batch Age(unit: milliseconds)";
        String type = "integer";
        JsonNode defaultValue = new IntNode(0);
    }

    @Getter
    static public class EnableBackgroundFlush extends BufferProperty {
        String name = ENABLE_BACKGROUND_FLUSH;
        String uiShowName = "Enable Background Flush";
        String type = "boolean";
        JsonNode defaultValue = BooleanNode.getFalse();
        List<JsonNode> enumValues = List.of(BooleanNode.getFalse(), BooleanNode.getTrue());
    }
}
