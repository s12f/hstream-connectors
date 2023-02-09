package io.hstream.io;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.networknt.schema.JsonSchemaFactory;
import com.networknt.schema.SpecVersion;
import io.hstream.HRecord;
import io.hstream.io.impl.ChannelKvStore;
import io.hstream.io.impl.StdioChannel;
import io.hstream.io.internal.Channel;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import lombok.extern.slf4j.Slf4j;

@Parameters(
        commandNames = "spec",
        commandDescription = "get configuration specification"
)
class SpecCmd {}

@Parameters(
    commandNames = "check",
    commandDescription = "check the connector configuration before run the task"
)
class CheckCmd {
    @Parameter(
        names = "--config",
        description = "configuration file path",
        required = true
    )
    String configPath;
}

@Parameters(
    commandNames = "run",
    commandDescription = "run the task"
)
class RunCmd {
    @Parameter(
        names = "--config",
        description = "configuration file path",
        required = true
    )
    String configPath;
}

@Slf4j
public class TaskRunner {
    TaskContext ctx;
    // for json schema verifier only
    JsonNode cfgNode;
    HRecord cfg;
    Task task;
    Channel channel;
    KvStore kv;

    public void run(String[] args, Task task, TaskContext taskContext) {
        log.info("task runner started");
        this.task = task;
        this.ctx = taskContext;
        this.channel = new StdioChannel();
        this.kv = new ChannelKvStore(channel);
        var checkCmd = new CheckCmd();
        var runCmd = new RunCmd();
        var jc = JCommander.newBuilder()
                .addCommand(new SpecCmd())
                .addCommand(checkCmd)
                .addCommand(runCmd)
                .build();
        try {
            jc.parse(args);
        } catch (ParameterException e) {
            log.error(e.getMessage());
            jc.usage();
            System.exit(1);
        }
        switch (jc.getParsedCommand()) {
            case "spec":
                channel.send(task.spec());
                break;
            case "check":
                parseConfig(checkCmd.configPath);
                check(task);
                break;
            case "run":
                parseConfig(runCmd.configPath);
                new Thread(this::recvCmd).start();
                var connectorConfig = cfg.getHRecord("connector");
                try {
                    parseConfig(runCmd.configPath);
                    ctx.init(cfg, kv);
                    if (task instanceof SourceTask) {
                        var st = (SourceTask) task;
                        var stc = (SourceTaskContext) taskContext;
                        st.run(connectorConfig, stc);
                    } else {
                        var st = (SinkTask) task;
                        var stc = (SinkTaskContext) taskContext;
                        st.run(connectorConfig, stc);
                    }
                } catch (Exception e) {
                    log.info("unexpected error when running connector:{}", e.getMessage());
                    e.printStackTrace(System.out);
                    Utils.runWithTimeout(3, this::stop);
                }
                break;
            default:
                log.error("invalid cmd:{}", jc.getParsedCommand());
        }
    }

    void check(Task task) {
        var schema = JsonSchemaFactory.getInstance(SpecVersion.VersionFlag.V202012).getSchema(task.spec());
        var errors = schema.validate(cfgNode.get("connector"));
        var mapper = new ObjectMapper();
        if (errors.size() == 0) {
            var msg = mapper.createObjectNode()
                    .put("result", true)
                    .put("message", "ok");
            System.out.println(msg);
//            channel.send(msg);
        } else {
            log.info("check failed:{}", errors);
            var msg = mapper.createObjectNode()
                    .put("result", false)
                    .put("message", "config check failed:" + errors);
            System.out.println(msg);
//            channel.send(msg);
        }
        System.out.flush();
    }

    void parseConfig(String cfgPath) {
        log.info("config file path:{}", cfgPath);
        try {
            var cfgText = Files.readString(Paths.get(cfgPath));
            this.cfg = HRecord.newBuilder().merge(cfgText).build();
            this.cfgNode = new ObjectMapper().readTree(cfgText);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


    public void recvCmd() {
        log.info("receiving commands");
        channel.handle(msg -> {
            var cmdType = msg.get("type").asText();
            if (cmdType.equals("stop")) {
                Utils.runWithTimeout(3, this::stop);
                System.exit(0);
            }
        });
    }

    public void stop() {
        log.info("stopping runner");
        if (task instanceof SourceTask) {
            task.stop();
            log.info("stopped source task");
            ctx.close();
            log.info("stopped source context");
        } else {
            ctx.close();
            log.info("stopped sink context");
            task.stop();
            log.info("stopped sink task");
        }
        log.info("stopped runner");
        try {
            kv.close();
        } catch (InterruptedException ignored) {}
    }
}