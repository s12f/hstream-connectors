package source.debezium;

import io.hstream.HRecord;
import io.hstream.io.SourceTaskContext;
import io.hstream.io.TaskRunner;
import io.hstream.io.impl.SourceTaskContextImpl;

public class PostgresqlSourceTask extends DebeziumSourceTask {
    @Override
    public void run(HRecord cfg, SourceTaskContext ctx) {
        var dbname = cfg.getString("database");
        var table = cfg.getString("table");
        props.setProperty("connector.class", "io.debezium.connector.postgresql.PostgresConnector");
        props.setProperty("database.dbname", dbname);
        if (table.split("\\.").length == 1) {
            props.setProperty("table.include.list", "public." + table);
        }
        super.run(cfg, ctx);
    }

    @Override
    public String spec() {
        return io.hstream.io.Utils.getSpec(this, "/source_postgresql_spec.json");
    }

    public static void main(String[] args) {
        var ctx = new SourceTaskContextImpl();
        new TaskRunner().run(args, new PostgresqlSourceTask(), ctx);
    }
}
