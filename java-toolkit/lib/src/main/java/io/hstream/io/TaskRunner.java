package io.hstream.io;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import io.hstream.HRecord;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Scanner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    SourceTaskContext ctx;
    HRecord cfg;
    SourceTask sourceTask;

    Logger logger = LoggerFactory.getLogger(TaskRunner.class);

    public TaskRunner(String[] args, SourceTask task, SourceTaskContext sourceTaskContext) {
        this.sourceTask = task;
        this.ctx = sourceTaskContext;
        var checkCmd = new CheckCmd();
        var runCmd = new RunCmd();
        var jc = JCommander.newBuilder()
                .addCommand(new CheckCmd())
                .addCommand(new RunCmd())
                .build();
        try {
            jc.parse(args);
            switch (jc.getParsedCommand()) {
                case "check":
                    parseConfig(checkCmd.configPath);
                    break;
                case "run":
                    parseConfig(runCmd.configPath);
                    break;
                default:
                    logger.error("invalid cmd:{}", jc.getParsedCommand());
            }
        } catch (Exception e) {
            jc.usage();
            System.exit(1);
        }
    }

    void parseConfig(String cfgPath) throws IOException {
        String cfgText = Files.readString(Paths.get(cfgPath));
        this.cfg = HRecord.newBuilder().merge(cfgText).build();
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

    public void run() {
        ctx.init(cfg);
        sourceTask.init(cfg.getHRecord("connector"), ctx);
        new Thread(this::recvCmd).start();
        sourceTask.run();
    }

    public void stop() throws Exception {
        sourceTask.stop();
        ctx.close();
    }
}

