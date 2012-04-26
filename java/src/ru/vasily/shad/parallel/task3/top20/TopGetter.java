package ru.vasily.shad.parallel.task3.top20;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import ru.vasily.shad.parallel.task3.ToolFactory;

public class TopGetter implements ToolFactory
{
    private final TopGetterTool.ReducerCount reducerCount;

    public TopGetter(TopGetterTool.ReducerCount reducerCount)
    {
        this.reducerCount = reducerCount;
    }

    @Override
    public Tool createTool(Path inputPath, Path outputPath)
    {
        return new TopGetterTool(inputPath, outputPath, reducerCount);
    }

    @Override
    public String getToolName()
    {
        return "top_getter";
    }
}
