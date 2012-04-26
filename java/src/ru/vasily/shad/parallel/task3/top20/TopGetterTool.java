package ru.vasily.shad.parallel.task3.top20;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;

import java.io.IOException;
import java.util.StringTokenizer;
import static ru.vasily.shad.parallel.task3.ShadParallelConstants.*;

public class TopGetterTool extends Configured implements Tool
{

    private final Path inputPath;
    private final Path outputPath;
    private final ReducerCount reducerCount;

    public enum ReducerCount
    {
        ONE, MANY
    }

    public TopGetterTool(Path inputPath, Path outputPath, ReducerCount reducerCount)
    {
        this.inputPath = inputPath;
        this.outputPath = outputPath;
        this.reducerCount = reducerCount;
    }

    public static class Map
            extends Mapper<LongWritable, Text, LongWritable, Text>
    {
        private final static LongWritable longWritable = new LongWritable();
        private Text textWritable = new Text();

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException
        {
            String line = value.toString();
            StringTokenizer tokenizer = new StringTokenizer(line);
            String word = tokenizer.nextToken();
            int occasionsNumber = Integer.valueOf(tokenizer.nextToken());
            textWritable.set(word);
            longWritable.set(occasionsNumber);
            context.write(longWritable, textWritable);
        }
    }

    public static class Reduce extends Reducer<LongWritable, Text, Text, LongWritable>
    {

        private int count = 0;

        public void reduce(LongWritable key, Iterable<Text> values,
                           Context context) throws IOException, InterruptedException
        {
            int numberOfTopElements = context.getConfiguration().getInt(NUMBER_OF_TOP_WORDS_KEY, 1);
            if (count == numberOfTopElements)
            {
                return;
            }
            for (Text word : values)
            {
                context.write(word, key);
                count++;
                if (count == numberOfTopElements)
                {
                    break;
                }
            }
        }
    }

    @Override
    public int run(String[] strings) throws Exception
    {
        Configuration conf = getConf();
        Job job = new Job(conf);
        job.setJarByClass(WordCounterTool.class);
        job.setJobName("topGetter");

        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setSortComparatorClass(LongWritable.DecreasingComparator.class);
        int numberOfReducers = reducerCount.equals(ReducerCount.MANY) ?
                getConf().getInt(REDUCERS_NUMBER_KEY, 1) : 1;
        job.setNumReduceTasks(numberOfReducers);

        FileInputFormat.setInputPaths(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        boolean success = job.waitForCompletion(true);
        return success ? 0 : 1;
    }
}
