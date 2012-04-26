package ru.vasily.shad.parallel.task3;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import ru.vasily.shad.parallel.task3.index.InversedIndexTool;
import ru.vasily.shad.parallel.task3.index.WordFrequencyTool;

public class Index
{
    public static void main(String[] args) throws Exception
    {
        Path inputPath = new Path(args[args.length - 2]);
        Path outputPath = new Path(args[args.length - 1]);
        Path wordFreqOutputPath = new Path(outputPath, "0_word_frequency");
        WordFrequencyTool wordFrequencyTool =
                new WordFrequencyTool(inputPath, wordFreqOutputPath);
        ToolRunner.run(wordFrequencyTool, args);

        long articlesCount = wordFrequencyTool.getArticlesCount();
        Tool inversedIndexTool =
                new InversedIndexTool(wordFreqOutputPath,
                        new Path(outputPath, "1_inversed_index"),
                        articlesCount);
        ToolRunner.run(inversedIndexTool, args);
    }
}
