package source.mysql;

import io.hstream.debezium.DebeziumSourceTaskContext;
import java.io.IOException;
import io.hstream.TaskRunner;

public class Main {
    public static void main(String[] args) throws IOException {
        var ctx = new DebeziumSourceTaskContext();
        new TaskRunner(args, new MySQLSourceTask(), ctx).run();
    }
}
