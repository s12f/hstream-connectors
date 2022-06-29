package io.hstream.io;

import io.hstream.HRecord;
import io.hstream.io.SourceTask;
import io.hstream.io.SourceTaskContext;
import java.nio.file.Files;
import java.nio.file.Paths;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

public class TaskRunner {
    SourceTaskContext ctx;
    HRecord cfg;
    SourceTask sourceTask;

    public TaskRunner(String[] args, SourceTask task, SourceTaskContext sourceTaskContext) {
        this.sourceTask = task;
        this.ctx = sourceTaskContext;
        var options = new Options();
        options.addOption(new Option("config", true, "task config file"));
        CommandLineParser parser = new DefaultParser();
        HelpFormatter formatter = new HelpFormatter();

        try {
            var cmd = parser.parse(options, args);
            String cfgText = Files.readString(Paths.get(cmd.getOptionValue("config")));
            this.cfg = HRecord.newBuilder().merge(cfgText).build();
        } catch (Exception e) {
            System.out.println(e.getMessage());
            formatter.printHelp("utility-name", options);
            System.exit(1);
        }
    }

    public void run() {
        ctx.init(cfg);
        sourceTask.init(cfg.getHRecord("connector"), ctx);
        sourceTask.run();
    }
}

