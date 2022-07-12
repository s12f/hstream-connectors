package io.hstream.io;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;
import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.networknt.schema.JsonSchemaFactory;
import com.networknt.schema.SpecVersion;
import io.hstream.HRecord;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Scanner;
import java.util.stream.Collectors;
import org.everit.json.schema.Schema;
import org.everit.json.schema.ValidationException;
import org.everit.json.schema.loader.SchemaLoader;
import org.json.JSONObject;
import org.json.JSONTokener;
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
                    ctx.init(cfg);
                    if (task instanceof SourceTask) {
                        ((SourceTask)task).init(cfg.getHRecord("connector"), (SourceTaskContext) taskContext);
                    }
                    new Thread(this::recvCmd).start();
                    task.run();
                    break;
                default:
                    logger.error("invalid cmd:{}", jc.getParsedCommand());
            }
        } catch (ParameterException e) {
            logger.error(e.getMessage());
            jc.usage();
            System.exit(1);
        } catch (IOException e) {
            throw new RuntimeException(e);
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

    void parseConfig(String cfgPath) throws IOException {
        logger.info("config file path:{}", cfgPath);
        String cfgText = Files.readString(Paths.get(cfgPath));
        this.cfg = HRecord.newBuilder().merge(cfgText).build();
        this.cfgNode = new ObjectMapper().readTree(cfgText);
    }


    public void recvCmd() {
        Scanner scan=new Scanner(System.in);
        while (true) {
            var line = scan.nextLine();
            try {
                var cmd = HRecord.newBuilder().merge(line).build();
                var cmdType = cmd.getString("type");
                if (cmdType.equals("stop")) {
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

    public void stop() throws Exception {
        task.stop();
        ctx.close();
    }
}

