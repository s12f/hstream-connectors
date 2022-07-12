package source.debezium;


import io.debezium.document.DocumentReader;
import io.debezium.document.DocumentWriter;
import io.debezium.relational.history.AbstractDatabaseHistory;
import io.debezium.relational.history.DatabaseHistoryException;
import io.debezium.relational.history.HistoryRecord;
import io.hstream.io.KvStore;
import java.io.IOException;
import java.util.function.Consumer;

public class DatabaseHistory extends AbstractDatabaseHistory {
    static KvStore kv;
    DocumentWriter writer = DocumentWriter.defaultWriter();
    DocumentReader reader = DocumentReader.defaultReader();

    public static void setKv(KvStore kv) {
        DatabaseHistory.kv = kv;
    }

    @Override
    protected void storeRecord(HistoryRecord record) throws DatabaseHistoryException {
        try {
            String line = writer.write(record.document());
            var hc = kv.get("history_count");
            if (hc == null) {
                kv.set("history_0", line);
                kv.set("history_count", "1");
            } else {
                var hcInt = Integer.parseInt(hc);
                kv.set("history_" + hcInt, line);
                kv.set("history_count", String.valueOf(hcInt + 1));
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected void recoverRecords(Consumer<HistoryRecord> records) {
        var hc = kv.get("history_count");
        if (hc == null) {
            return;
        }
        for (int i = 0; i < Integer.parseInt(hc); i++) {
            var val = kv.get("history_" + i);
            try {
                records.accept(new HistoryRecord(reader.read(val)));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public boolean exists() {
        return false;
    }

    @Override
    public boolean storageExists() {
        return false;
    }
}
