package source.debezium;

import com.fasterxml.jackson.databind.JsonNode;
import io.hstream.HRecord;
import io.hstream.io.SourceTaskContext;
import io.hstream.io.TaskRunner;
import io.hstream.io.impl.SourceTaskContextImpl;
import java.util.UUID;

public class PostgresqlSourceTask extends RdbSourceTask {
    @Override
    public void run(HRecord cfg, SourceTaskContext ctx) {
        var dbname = cfg.getString("database");
        var table = cfg.getString("table");
        props.setProperty("connector.class", "io.debezium.connector.postgresql.PostgresConnector");
        props.setProperty("database.dbname", dbname);
        props.setProperty("table.include.list", table);
        props.setProperty("plugin.name", "pgoutput");
        props.setProperty("slot.name", "hstream_" + UUID.randomUUID().toString().replace("-", ""));
        if (table.split("\\.").length == 1) {
            props.setProperty("table.include.list", "public." + table);
        }
        super.run(cfg, ctx);
    }

    @Override
    public JsonNode spec() {
        return io.hstream.io.Utils.getSpec(this, "/source_postgresql_spec.json");
    }

    public static void main(String[] args) {
        var ctx = new SourceTaskContextImpl();
        new TaskRunner().run(args, new PostgresqlSourceTask(), ctx);
    }
}
