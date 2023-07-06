package sink.elasticsearch;

import lombok.Builder;
import lombok.Data;
import org.apache.http.HttpHost;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Data
@Builder
public class ConnectionUrl {
    String schema;
    List<Map.Entry<String, Integer>> hosts;

    public List<HttpHost> toHttps() {
        return hosts.stream()
                .map(host -> new HttpHost(host.getKey(), host.getValue(), schema))
                .collect(Collectors.toList());
    }

    public static ConnectionUrl fromUrlString(String url) {
        String uriStr = url.strip();
        var schemaHosts = uriStr.split("://");
        if (schemaHosts.length != 2) {
            throw new IllegalArgumentException(
                    "incorrect serviceUrl: " + uriStr + " (correct example: http://host1:9200)");
        }
        var schemaStr = schemaHosts[0];
        var hosts = schemaHosts[1];
        try {
            return ConnectionUrl.builder()
                    .schema(schemaStr)
                    .hosts(parseHosts(hosts))
                    .build();
        } catch (Exception e) {
            throw new IllegalArgumentException(
                    "incorrect serviceUrl: " + uriStr + " (correct example: http://host1:9200)");
        }
    }

    static List<Map.Entry<String, Integer>> parseHosts(String hosts) {
        return Arrays.stream(hosts.split(","))
                .map(ConnectionUrl::parseHost)
                .collect(Collectors.toList());
    }

    static Map.Entry<String, Integer> parseHost(String host) {
        var address_port = host.split(":");
        if (address_port.length == 1) {
            return Map.entry(host, 9200);
        }
        if (address_port.length == 2) {
            return Map.entry(address_port[0], Integer.valueOf(address_port[1]));
        }
        throw new RuntimeException("invalid host:" + host);
    }
}
