package sink.jdbc;

import io.hstream.HRecord;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class Utils {
    static String makeFields(List<String> fields) {
        return String.join(", ", fields);
    }

    static String makeValues(int size) {
        return String.join(", ", Collections.nCopies(size, "?"));
    }

    static String makeUpsertUpdate(List<String> fields) {
        return fields.stream().map(s -> String.format("%s=VALUES(%s)", s, s)).collect(Collectors.joining(", "));
    }

    static String makeWhere(List<String> fields) {
        return fields.stream().map(s -> String.format("%s=?", s)).collect(Collectors.joining(", "));
    }

    static DB.Target parseTarget(String target) {
        var xs = target.split("\\.");
        if (xs.length != 2) {
            throw new RuntimeException("invalid target:" + target);
        }
        return new DB.Target(xs[0], xs[1]);
    }
}
