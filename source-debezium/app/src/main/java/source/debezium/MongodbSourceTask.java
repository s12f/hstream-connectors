package source.debezium;

import com.fasterxml.jackson.databind.JsonNode;
import io.hstream.HRecord;
import io.hstream.io.SourceTaskContext;
import io.hstream.io.TaskRunner;
import io.hstream.io.impl.SourceTaskContextImpl;

public class MongodbSourceTask extends DebeziumSourceTask {
    @Override
    public void run(HRecord cfg, SourceTaskContext ctx) {
        var hosts = cfg.getString("hosts");
        var dbname = cfg.getString("database");
        var collection = cfg.getString("collection");
        props.setProperty("connector.class", "io.debezium.connector.mongodb.MongoDbConnector");
        props.setProperty("mongodb.hosts", hosts);
        props.setProperty("mongodb.name", namespace);
        props.setProperty("collection.include.list", dbname + "." + collection);

        // transformer
        props.setProperty("transforms", "unwrap");
        props.setProperty("transforms.unwrap.type", "io.debezium.connector.mongodb.transforms.ExtractNewDocumentState");
        props.setProperty("transforms.unwrap.drop.tombstones", "false");
        props.setProperty("transforms.unwrap.delete.handling.mode", "drop");
        props.setProperty("transforms.unwrap.operation.header", "true");

        setKeyMapper(key -> HRecord.newBuilder().put("_id", key.getString("id")).build());

        super.run(cfg, ctx);
    }

    @Override
    public JsonNode spec() {
        return io.hstream.io.Utils.getSpec(this, "/source_mongodb_spec.json");
    }

    public static void main(String[] args) {
        var ctx = new SourceTaskContextImpl();
        new TaskRunner().run(args, new MongodbSourceTask(), ctx);
    }
}
