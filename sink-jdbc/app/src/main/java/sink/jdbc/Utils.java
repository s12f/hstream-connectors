package sink.jdbc;

import io.hstream.HRecord;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;
import lombok.SneakyThrows;

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

    @SneakyThrows
    static List<String> getPrimaryKey(Connection conn, String table) {
        DatabaseMetaData dmd = conn.getMetaData();
        ResultSet rs = dmd.getPrimaryKeys(null, null, table);

        var keys = new LinkedList<String>();
        while(rs.next()){
            keys.add(rs.getString("COLUMN_NAME"));
        }
        return keys;
    }

    static String makeWhere(List<String> fields) {
        return fields.stream().map(s -> String.format("%s=?", s)).collect(Collectors.joining(", "));
    }
}
