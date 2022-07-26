package source.debezium;

import io.hstream.HRecord;
import io.hstream.io.SourceTaskContext;
import io.hstream.io.TaskRunner;
import io.hstream.io.impl.SourceTaskContextImpl;

public class SqlserverSourceTask extends DebeziumSourceTask {
    @Override
    public void run(HRecord cfg, SourceTaskContext ctx) {
        var dbname = cfg.getString("database");
        var table = cfg.getString("table");
        props.setProperty("database.dbname", dbname);
        props.setProperty("connector.class", "io.debezium.connector.sqlserver.SqlServerConnector");
        props.setProperty("table.include.list", table);
        if (table.split("\\.").length == 1) {
            props.setProperty("table.include.list", "dbo." + table);
        }
        super.run(cfg, ctx);
    }

    @Override
    public String spec() {
        return io.hstream.io.Utils.getSpec(this, "/source_sqlserver_spec.json");
    }

    public static void main(String[] args) {
        var ctx = new SourceTaskContextImpl();
        new TaskRunner().run(args, new SqlserverSourceTask(), ctx);
    }
}
