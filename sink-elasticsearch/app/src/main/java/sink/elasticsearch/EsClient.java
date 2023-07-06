package sink.elasticsearch;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.BulkRequest;
import co.elastic.clients.elasticsearch.core.search.Hit;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import co.elastic.clients.util.BinaryData;
import co.elastic.clients.util.ContentType;
import com.fasterxml.jackson.databind.JsonNode;
import io.hstream.io.SinkRecord;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;

import java.util.List;

@Slf4j
public class EsClient {
    ConnectionUrl connectionUrl;
    ElasticsearchClient esClient;

    public EsClient(ConnectionUrl url) {
        connectionUrl = url;
    }

    public EsClient(String url) {
        connectionUrl = ConnectionUrl.fromUrlString(url);
    }

    ElasticsearchClient getConnection() {
        if (esClient == null) {
            RestClient restClient = RestClient.builder(connectionUrl.toHttps().toArray(new HttpHost[0])).build();
            ElasticsearchTransport transport = new RestClientTransport(
                    restClient, new JacksonJsonpMapper());
            esClient = new ElasticsearchClient(transport);
        }
        return esClient;
    }

    void reset() {
        esClient = null;
    }

    @SneakyThrows
    void writeRecords(String index, List<SinkRecord> records) {
        var client = getConnection();
        BulkRequest.Builder br = new BulkRequest.Builder();
        for (var record : records) {
            BinaryData data = BinaryData.of(record.record, ContentType.APPLICATION_JSON);
            br.operations(op -> op.index(idx -> idx.index(index)
                    .id(record.getRecordId())
                    .document(data)));
        }
        try {
            client.bulk(br.build());
        } catch (Exception e) {
            log.warn("bulk records failed:{}", e.getMessage());
            reset();
            throw e;
        }
    }

    @SneakyThrows
    List<Hit<JsonNode>> readRecords(String index) {
        var client = getConnection();
        var resp = client.search(g -> g.index(index), JsonNode.class);
        return resp.hits().hits();
    }
}
