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
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Scanner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

public class TaskRunner {
    TaskContext ctx;
    // for json schema verifier only
    JsonNode cfgNode;
    HRecord cfg;
    Task task;
    Scanner scanner;
    String taskType = "source";

    Logger logger = LoggerFactory.getLogger(TaskRunner.class);

    public void run(String[] args, Task task, TaskContext taskContext) {
        this.task = task;
        this.ctx = taskContext;
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
            logger.error(e.getMessage());
            jc.usage();
            System.exit(1);
        }
        switch (jc.getParsedCommand()) {
            case "spec":
                send(task.spec());
                break;
            case "check":
                parseConfig(checkCmd.configPath);
                check(task);
                break;
            case "run":
                parseConfig(runCmd.configPath);

                scanner = new Scanner(System.in);
                new Thread(this::recvCmd).start();

                var connectorConfig = cfg.getHRecord("connector");
                try {
                    parseConfig(runCmd.configPath);
                    if (task instanceof SourceTask) {
                        var st = (SourceTask) task;
                        var stc = (SourceTaskContext) taskContext;
                        stc.init(cfg);
                        st.run(connectorConfig, stc);
                    } else {
                        var st = (SinkTask) task;
                        var stc = (SinkTaskContext) taskContext;
                        stc.init(cfg, st);
                        st.init(connectorConfig, stc);
                        stc.run();
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    stop();
                }
                break;
            default:
                logger.error("invalid cmd:{}", jc.getParsedCommand());
        }
    }

    void check(Task task) {
        var schema = JsonSchemaFactory.getInstance(SpecVersion.VersionFlag.V202012).getSchema(task.spec());
        var errors = schema.validate(cfgNode.get("connector"));
        if (errors.size() == 0) {
            var msg = HRecord.newBuilder()
                    .put("result", true)
                    .put("message", "ok")
                    .build()
                    .toCompactJsonString();
            send(msg);
        } else {
            logger.info("check failed:{}", errors);
            var msg = HRecord.newBuilder()
                    .put("result", false)
                    .put("message", "config check failed:" + errors)
                    .build()
                    .toCompactJsonString();
            send(msg);
        }
    }

    void parseConfig(String cfgPath) {
        logger.info("config file path:{}", cfgPath);
        try {
            var cfgText = Files.readString(Paths.get(cfgPath));
            this.cfg = HRecord.newBuilder().merge(cfgText).build();
            this.cfgNode = new ObjectMapper().readTree(cfgText);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


    public void recvCmd() {
        logger.info("receiving commands");
        while (true) {
            var line = scanner.nextLine();
            logger.info("received line:{}", line);
            try {
                var cmd = HRecord.newBuilder().merge(line).build();
                var cmdType = cmd.getString("type");
                if (cmdType.equals("stop")) {
                    stop();
                    break;
                }
            } catch (Exception e) {
                logger.warn("received an invalid cmd:{}", line);
            }
        }
    }

    void send(String msg) {
        System.out.println(msg);
        System.out.flush();
    }

    public void stop() {
        logger.info("stopping runner");
        scanner.close();
        logger.info("stopped scanner");
        if (task instanceof SourceTask) {
            task.stop();
            logger.info("stopped task");
            ctx.close();
            logger.info("stopped context");
        } else {
            ctx.close();
            logger.info("stopped context");
            task.stop();
            logger.info("stopped task");
        }
        logger.info("stopped runner");
    }
}