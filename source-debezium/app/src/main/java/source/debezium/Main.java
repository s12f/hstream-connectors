package source.debezium;

import io.hstream.io.impl.SourceTaskContextImpl;
import java.io.IOException;
import io.hstream.io.TaskRunner;

public class Main {
    public static void main(String[] args) throws IOException {
        var ctx = new SourceTaskContextImpl();
        new TaskRunner(args, new DebeziumSourceTask(), ctx).run();
    }
}
