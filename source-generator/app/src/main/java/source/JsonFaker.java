package source;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.SneakyThrows;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Supplier;

public class JsonFaker {
    static ObjectMapper mapper = new ObjectMapper();
    Map<String, Supplier<Object>> objectGenerator = new HashMap<>();
    Random rnd = new Random();

    @SneakyThrows
    public JsonFaker(String schema) {
        var json = mapper.readTree(schema);
        var objectJson = json.get("properties");
        objectJson.fieldNames().forEachRemaining(key -> {
            var type = objectJson.get(key).get("type").asText().toLowerCase();
            switch (type) {
                case "integer":
                    var min = objectJson.get("minimum") == null ? 0 : objectJson.get("minimum").asInt();
                    var max = objectJson.get("maximum") == null ? 1000000 : objectJson.get("maximum").asInt();
                    objectGenerator.put(key, () -> ThreadLocalRandom.current().nextInt(min, max));
                    break;
                case "string":
                    objectGenerator.put(key, UUID::randomUUID);
                    break;
                default:
                    throw new UnsupportedOperationException("unsupported type:" + type);
            }
        });
    }

    @SneakyThrows
    public String generate() {
        var result = new HashMap<String, Object>();
        for (var entry : objectGenerator.entrySet()) {
            result.put(entry.getKey(), entry.getValue().get());
        }
        return mapper.writeValueAsString(result);
    }
}
