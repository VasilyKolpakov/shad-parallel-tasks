package ru.vasily.shad.parallel.task3.top20;

import com.google.common.base.CharMatcher;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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
import static ru.vasily.shad.parallel.task3.ShadParallelConstants.*;

import java.io.IOException;

import static com.google.common.base.CharMatcher.inRange;

public class WordCounterTool extends Configured implements Tool
{
    private final Path inputPath;
    private final Path outputPath;

    public WordCounterTool(Path inputPath, Path outputPath)
    {
        this.inputPath = inputPath;
        this.outputPath = outputPath;
    }

    public static class Map
            extends Mapper<LongWritable, Text, Text, IntWritable>
    {
        private static final CharMatcher englishSymbol =
                inRange('a', 'z').or(inRange('A', 'Z'));
        private static final CharMatcher russianSymbol =
                inRange('а', 'я').or(inRange('А', 'Я'));
        private static final java.util.Map<String, CharMatcher> charMatchers =
                ImmutableMap.<String, CharMatcher>builder()
                            .put("ru", russianSymbol)
                            .put("en", englishSymbol)
                            .put("all", russianSymbol.or(englishSymbol))
                            .build();

        private IntWritable one = new IntWritable(1);
        private Text textWritable = new Text();

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException
        {

            String articleWithName = value.toString();
            String articleWithoutName = articleWithName.substring(articleWithName.indexOf("\t") + 1);
            String languageMode = context.getConfiguration().get(LANGUAGE_MODE_KEY);
            CharMatcher wordCharMatcher = charMatchers.get(languageMode);
            Iterable<String> words = Splitter.on(wordCharMatcher.negate())
                                             .omitEmptyStrings()
                                             .split(articleWithoutName);
            for (String word : words)
            {
                textWritable.set(word.toLowerCase());
                context.write(textWritable, one);
            }
        }
    }

    public static class Reduce
            extends Reducer<Text, IntWritable, Text, IntWritable>
    {
        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context) throws IOException, InterruptedException
        {
            int sum = 0;
            for (IntWritable val : values)
            {
                sum += val.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

    public int run(String[] args) throws Exception
    {
        Configuration conf = getConf();
        Job job = new Job(conf);
        job.setJarByClass(WordCounterTool.class);
        job.setJobName("wordcount");

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapperClass(Map.class);
        job.setCombinerClass(Reduce.class);
        job.setReducerClass(Reduce.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        int numberOfReducers = getConf().getInt(REDUCERS_NUMBER_KEY, 1);
        job.setNumReduceTasks(numberOfReducers);

        FileInputFormat.setInputPaths(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);
        boolean success = job.waitForCompletion(true);
        return success ? 0 : 1;
    }

}
