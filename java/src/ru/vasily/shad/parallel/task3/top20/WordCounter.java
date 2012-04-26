package ru.vasily.shad.parallel.task3.top20;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import ru.vasily.shad.parallel.task3.ToolFactory;

public class WordCounter implements ToolFactory
{

    @Override
    public Tool createTool(Path inputPath, Path outputPath)
    {
        return new WordCounterTool(inputPath, outputPath);
    }

    @Override
    public String getToolName()
    {
        return "word_counter";
    }
}
