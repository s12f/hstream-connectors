package source.debezium;

import io.hstream.HRecord;
import io.hstream.io.SourceTaskContext;

public abstract class RdbSourceTask extends DebeziumSourceTask {
    @Override
    public void run(HRecord cfg, SourceTaskContext ctx) {
        // database
        props.setProperty("database.hostname", cfg.getString("host"));
        props.setProperty("database.port", String.valueOf(cfg.getInt("port")));
        props.setProperty("database.user", cfg.getString("user"));
        props.setProperty("database.password", cfg.getString("password"));
        props.setProperty("database.server.name", getNamespace(ctx.getKvStore()));

        // transforms
        props.setProperty("transforms", "unwrap");
        props.setProperty("transforms.unwrap.type", "io.debezium.transforms.ExtractNewRecordState");
        props.setProperty("transforms.unwrap.drop.tombstones", "false");

        super.run(cfg, ctx);
    }
}
