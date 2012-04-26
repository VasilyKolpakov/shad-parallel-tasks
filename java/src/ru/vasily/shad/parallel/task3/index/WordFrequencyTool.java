package ru.vasily.shad.parallel.task3.index;

import com.google.common.base.CharMatcher;
import com.google.common.base.Function;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;

import javax.annotation.Nullable;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;

import static com.google.common.base.CharMatcher.inRange;
import static com.google.common.collect.Iterables.transform;
import static ru.vasily.shad.parallel.task3.ShadParallelConstants.LANGUAGE_MODE_KEY;

public class WordFrequencyTool extends Configured implements Tool
{

    public static final String TOP_WORDS_KEY = "topWords";
    public static final String TOP_PATH_KEY = "topPath";

    public long getArticlesCount()
    {
        return articlesCount;
    }

    private enum Counters
    {
        ARTICLE_COUNTER
    }

    private final Path inputPath;
    private final Path outputPath;

    private long articlesCount;

    public WordFrequencyTool(Path inputPath, Path outputPath)
    {
        this.inputPath = inputPath;
        this.outputPath = outputPath;
    }

    public static class Map
            extends Mapper<LongWritable, Text, Text, ArticleNameAndFrequency>
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

        private Text textWritable = new Text();
        private ArticleNameAndFrequency articleNameAndFrequency = new ArticleNameAndFrequency();

        private Set<String> topWords;

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException
        {
            HashMap<String, Integer> wordCounts = new HashMap<String, Integer>();
            String article = value.toString();
            String articleName = article.substring(0, article.indexOf("\t"));
            String articleContent = article.substring(article.indexOf("\t") + 1);
            Iterable<String> words = parseWords(context, articleContent);
            int wordCount = 0;

            for (String word : words)
            {
                if (!topWords.contains(word))
                {
                    addCount(wordCounts, word);
                }
                wordCount++;
            }

            for (java.util.Map.Entry<String, Integer> wordAndCount : wordCounts.entrySet())
            {
                double frequency = ((double) wordAndCount.getValue()) / wordCount;
                String word = wordAndCount.getKey();
                textWritable.set(word);
                articleNameAndFrequency.set(articleName, frequency);
                context.write(textWritable, articleNameAndFrequency);
            }
            context.getCounter(Counters.ARTICLE_COUNTER).increment(1);
        }

        @Override
        protected void setup(Context context) throws IOException, InterruptedException
        {
            super.setup(context);
            String[] topWordsArr = context.getConfiguration().getStrings(TOP_WORDS_KEY);
            topWords = ImmutableSet.copyOf(topWordsArr);
        }

        private void addCount(HashMap<String, Integer> wordCounts, String word)
        {
            Integer wordCount = wordCounts.get(word);
            if (wordCount == null)
            {
                wordCounts.put(word, 1);
            }
            else
            {
                wordCounts.put(word, wordCount + 1);
            }
        }

        private Iterable<String> parseWords(Context context, String articleContent)
        {
            String languageMode = context.getConfiguration().get(LANGUAGE_MODE_KEY);
            CharMatcher wordCharMatcher = charMatchers.get(languageMode);
            Iterable<String> rawWords = Splitter.on(wordCharMatcher.negate())
                                                .omitEmptyStrings()
                                                .split(articleContent);
            return transform(rawWords, new Function<String, String>()
            {
                @Override
                public String apply(@Nullable String input)
                {
                    return input.toLowerCase();
                }
            });
        }
    }

    public int run(String[] args) throws Exception
    {
        Configuration conf = getConf();
        Collection<String> topWords = parseTopWords(conf);
        String[] topWordsArr = topWords.toArray(new String[topWords.size()]);
        conf.setStrings(TOP_WORDS_KEY, topWordsArr);

        Job job = new Job(conf);
        job.setJarByClass(InversedIndexTool.class);
        job.setJobName("inversed_index");

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(ArticleNameAndFrequency.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(ArticleNameAndFrequency.class);

        job.setMapperClass(Map.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setNumReduceTasks(0);

        FileInputFormat.setInputPaths(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);
        boolean success = job.waitForCompletion(true);

        articlesCount = job.getCounters().findCounter(Counters.ARTICLE_COUNTER).getValue();
        return success ? 0 : 1;
    }

    private Collection<String> parseTopWords(Configuration conf) throws IOException
    {
        String topPathStr = conf.get(TOP_PATH_KEY);
        if (topPathStr == null)
        {
            throw new IllegalArgumentException("topPath is not specified");
        }
        Path topPath = new Path(topPathStr);
        FileSystem fileSystem = FileSystem.get(conf);
        FSDataInputStream stream = fileSystem.open(topPath);
        BufferedReader reader = new BufferedReader(new InputStreamReader(stream));
        Collection<String> topWords = new ArrayList<String>();
        String line;
        while ((line = reader.readLine()) != null)
        {
            String[] wordAndCount = line.split("\t");
            topWords.add(wordAndCount[0]);
        }
        return topWords;
    }
}

