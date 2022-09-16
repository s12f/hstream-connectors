package sink.mongodb;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.ReplaceOneModel;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.model.UpdateManyModel;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.UpdateOptions;
import io.hstream.HRecord;
import io.hstream.io.SinkRecord;
import io.hstream.io.SinkTask;
import io.hstream.io.SinkTaskContext;
import io.hstream.io.TaskRunner;
import io.hstream.io.Utils;
import io.hstream.io.impl.SinkTaskContextImpl;
import java.util.List;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.bson.Document;

@Slf4j
public class MongodbSinkTask implements SinkTask {
    HRecord cfg;
    MongoClient client;
    MongoCollection<Document> collection;

    @Override
    public void init(HRecord cfg, SinkTaskContext sinkTaskContext) {
        this.cfg = cfg;
        var hosts = cfg.getString("hosts");
        var dbStr = cfg.getString("database");
        var collectionStr = cfg.getString("collection");
        var authString = "";
        if (cfg.contains("user")) {
            var user = cfg.getString("user");
            var password = cfg.getString("password");
            authString = String.format("%s:%s@", user, password);
        }
        var connStr = String.format("mongodb://%s%s/", authString, hosts);
        client = MongoClients.create(connStr);
        var db = client.getDatabase(dbStr);
        collection = db.getCollection(collectionStr);
    }

    @Override
    public void send(String s, List<SinkRecord> records) {
        var result = collection.bulkWrite(records.stream().map(this::mapRecord).collect(Collectors.toList()));
        log.info("bulkWrite result:{}", result);
    }

    @Override
    public String spec() {
        return Utils.getSpec(this, "/spec.json");
    }

    @Override
    public void stop() {
        client.close();
    }

    private UpdateOneModel<Document> mapRecord(SinkRecord sinkRecord) {
        var record = sinkRecord.record;
        var keyDoc = Document.parse(record.getHRecord("key").toCompactJsonString());
        var valDoc = Document.parse(record.getHRecord("value").toCompactJsonString());
        log.info("keyDoc:{}, valDoc:{}", keyDoc, valDoc);
        return new UpdateOneModel<>(keyDoc,
                new Document("$set", valDoc),
                new UpdateOptions().upsert(true));
    }

    public static void main(String[] args) {
        new TaskRunner().run(args, new MongodbSinkTask(), new SinkTaskContextImpl());
    }
}
