package ru.vasily.shad.parallel.task3;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import ru.vasily.shad.parallel.task3.top20.TopGetter;
import ru.vasily.shad.parallel.task3.top20.TopGetterTool;
import ru.vasily.shad.parallel.task3.top20.WordCounter;

public class Top20
{
    public static void main(String[] args) throws Exception
    {
        Path inputPath = new Path(args[args.length - 2]);
        Path outputPath = new Path(args[args.length - 1]);
        runChainedJobs(inputPath, outputPath, args,
                new WordCounter(),
                new TopGetter(TopGetterTool.ReducerCount.MANY),
                new TopGetter(TopGetterTool.ReducerCount.ONE)
                      );
    }

    public static void runChainedJobs(Path inputPath, Path outputPath, String[] args, ToolFactory... toolFactories) throws Exception
    {
        Path toolInputPath = inputPath;
        int toolCount = 0;
        for (ToolFactory toolFactory : toolFactories)
        {
            String outputFolderName = folderName(toolCount, toolFactory);
            Path toolOutputPath = new Path(outputPath, outputFolderName);
            Tool tool = toolFactory.createTool(toolInputPath, toolOutputPath);
            int counterErrCode = ToolRunner.run(tool, args);
            if (counterErrCode == 1)
            {
                System.exit(1);
            }
            toolCount++;
            toolInputPath = toolOutputPath;
        }
    }

    private static String folderName(int jobNumber, ToolFactory tool)
    {
        return String.format("%d_%s", jobNumber, tool.getToolName());
    }


}
