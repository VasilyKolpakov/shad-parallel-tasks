package ru.vasily.shad.parallel.task3;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;

public interface ToolFactory
{
    Tool createTool(Path inputPath, Path outputPath);

    String getToolName();
}
