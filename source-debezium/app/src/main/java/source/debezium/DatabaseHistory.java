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
            logger.info("storing record:{}", line);
            var hc = kv.get("history_count").join();
            if (hc == null) {
                kv.set("history_0", line).join();
                kv.set("history_count", "1").join();
            } else {
                var hcInt = Integer.parseInt(hc);
                kv.set("history_" + hcInt, line).join();
                kv.set("history_count", String.valueOf(hcInt + 1)).join();
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected void recoverRecords(Consumer<HistoryRecord> records) {
        var hc = kv.get("history_count").join();
        if (hc == null) {
            logger.info("history_count is null");
            return;
        }
        for (int i = 0; i < Integer.parseInt(hc); i++) {
            var val = kv.get("history_" + i).join();
            try {
                records.accept(new HistoryRecord(reader.read(val)));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public boolean exists() {
        if (!storageExists()) {
            return false;
        }
        return kv.get("history_count").join() != null;
    }

    @Override
    public boolean storageExists() {
        return kv != null;
    }
}
