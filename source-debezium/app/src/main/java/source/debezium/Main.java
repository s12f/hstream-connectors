package source.debezium;

import io.hstream.io.impl.SourceTaskContextImpl;
import io.hstream.io.TaskRunner;

public class Main {
    public static void main(String[] args) {
        var ctx = new SourceTaskContextImpl();
        new TaskRunner().run(args, new DebeziumSourceTask(), ctx);
    }
}
