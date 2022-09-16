package io.hstream;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.hstream.internal.CreateConnectorRequest;
import io.hstream.internal.DeleteConnectorRequest;
import io.hstream.internal.HStreamApiGrpc;
import io.hstream.internal.LookupConnectorRequest;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.testcontainers.containers.DockerComposeContainer;

@Slf4j
public class HStreamHelper {
    String serverHost = "127.0.0.1";
    public HStreamClient client;
    private final Map<Integer, ManagedChannel> channels = new HashMap<>();
    private final Map<Integer, Integer> ports = new HashMap<>();
    public DockerComposeContainer<?> service;

    HStreamHelper() throws Exception {
        service = Utils.makeHStreamDB();
        service.start();
        var server1Port = service.getServicePort("hserver0", 6570);
        var server2Port = service.getServicePort("hserver1", 6572);
        ports.put(6570, server1Port);
        ports.put(6572, server2Port);
        client = HStreamClient.builder().serviceUrl(serverHost + ":" + server1Port).build();
        channels.put(server1Port, ManagedChannelBuilder.forAddress(serverHost, server1Port).usePlaintext().build());
        channels.put(server2Port, ManagedChannelBuilder.forAddress(serverHost, server2Port).usePlaintext().build());
        System.out.println("HStreamDB started");
    }


    void writeStream(String stream, List<HRecord> records) {
        if (client.listStreams().stream().noneMatch(s -> s.getStreamName().equals(stream))) {
            client.createStream(stream);
        }
        try (var producer = client.newBufferedProducer().stream(stream).build()) {
            records.forEach(r -> producer.write(Record.newBuilder().hRecord(r).build()));
        }
    }

    void close() throws Exception {
        channels.values().forEach(ManagedChannel::shutdown);
        client.close();
        service.stop();
    }

    void createConnector(String name, String sql) {
        var lookupReq = LookupConnectorRequest.newBuilder().setName(name).build();
        var lookupRes = getStub().lookupConnector(lookupReq);
        var res = getStub(ports.get(lookupRes.getServerNode().getPort()))
                .createConnector(CreateConnectorRequest.newBuilder().setSql(sql).build());
        log.info("create connector result:{}", res);
    }

    void deleteConnector(String name) {
        var lookupReq = LookupConnectorRequest.newBuilder().setName(name).build();
        var lookupRes = getStub().lookupConnector(lookupReq);
        getStub(ports.get(lookupRes.getServerNode().getPort()))
                .deleteConnector(DeleteConnectorRequest.newBuilder().setName(name).build());
        log.info("deleted connector", name);
    }

    HStreamApiGrpc.HStreamApiBlockingStub getStub() {
        return HStreamApiGrpc.newBlockingStub(channels.entrySet().iterator().next().getValue());
    }

    HStreamApiGrpc.HStreamApiBlockingStub getStub(int port) {
        var channel = channels.get(port);
        if (channel == null) {
            log.error("port:{}, channel ports:{}", port, channels.keySet());
            throw new RuntimeException("wrong channels");
        }
        return HStreamApiGrpc.newBlockingStub(channels.get(port));
    }
}
