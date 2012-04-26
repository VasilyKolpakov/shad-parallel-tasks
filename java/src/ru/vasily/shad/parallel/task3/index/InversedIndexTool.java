package ru.vasily.shad.parallel.task3.index;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
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

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static com.google.common.collect.Iterables.transform;
import static ru.vasily.shad.parallel.task3.ShadParallelConstants.REDUCERS_NUMBER_KEY;

public class InversedIndexTool extends Configured implements Tool
{
    private static final String NUMBER_OF_ARTICLES = "numberOfArticles";

    private enum Counters
    {
        ARTICLE_COUNTER
    }

    private final Path inputPath;
    private final Path outputPath;
    private final long numberOfArticles;

    public InversedIndexTool(Path inputPath, Path outputPath, long numberOfArticles)
    {
        this.inputPath = inputPath;
        this.outputPath = outputPath;
        this.numberOfArticles = numberOfArticles;
    }

    public static class Map
            extends Mapper<LongWritable, Text, Text, ArticleNameAndFrequency>
    {
        private Text textWritable = new Text();
        private ArticleNameAndFrequency articleNameAndFrequency = new ArticleNameAndFrequency();

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException
        {
            String[] wordArticleFreq = value.toString().split("\t");
            String word = wordArticleFreq[0];
            String articleName = wordArticleFreq[1];
            double frequency = Double.valueOf(wordArticleFreq[2]);
            textWritable.set(word);
            articleNameAndFrequency.set(articleName, frequency);
            context.write(textWritable, articleNameAndFrequency);
        }

    }

    public static class Reduce
            extends Reducer<Text, ArticleNameAndFrequency, Text, Text>
    {
        private long numberOfArticles;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException
        {
            super.setup(context);
            numberOfArticles = context.getConfiguration().getLong(NUMBER_OF_ARTICLES, 0);
        }

        public void reduce(Text key, Iterable<ArticleNameAndFrequency> values,
                           Context context) throws IOException, InterruptedException
        {
            List<ArticleNameAndTfIdt> articles = calculateIdfForArticles(values);
            Collections.sort(articles, ArticleNameAndTfIdt.BY_TFIDF_DESC);
            StringBuilder builder = new StringBuilder();
            for (ArticleNameAndTfIdt articleNameAndTfIdt : articles)
            {
                builder.append(articleNameAndTfIdt.articleName).append('\t');
                builder.append(articleNameAndTfIdt.tfIdf).append('\t');
            }
            context.write(key, new Text(builder.toString()));
        }

        private List<ArticleNameAndTfIdt> calculateIdfForArticles(Iterable<ArticleNameAndFrequency> values)
        {
            Iterable<ArticleNameAndFrequency> clonedValues = transform(values, new Function<ArticleNameAndFrequency, ArticleNameAndFrequency>()
            {
                @Override
                public ArticleNameAndFrequency apply(@Nullable ArticleNameAndFrequency input)
                {
                    return input.copy();
                }
            });
            List<ArticleNameAndFrequency> articleNamesAndFrecs = Lists.newArrayList(clonedValues);
            double idf = Math.log(((double) numberOfArticles) / articleNamesAndFrecs.size());
            List<ArticleNameAndTfIdt> articles = new ArrayList<ArticleNameAndTfIdt>();
            for (ArticleNameAndFrequency val : articleNamesAndFrecs)
            {
                double tfIdf = val.getFrequency() * idf;
                articles.add(new ArticleNameAndTfIdt(val.getArticleName(), tfIdf));
            }
            return articles;
        }
    }

    public int run(String[] args) throws Exception
    {
        Configuration conf = getConf();
        conf.setLong(NUMBER_OF_ARTICLES, numberOfArticles);
        Job job = new Job(conf);
        job.setJarByClass(InversedIndexTool.class);
        job.setJobName("inversed_index");

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(ArticleNameAndFrequency.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        int numberOfReducers = getConf().getInt(REDUCERS_NUMBER_KEY, 1);
        job.setNumReduceTasks(numberOfReducers);

        FileInputFormat.setInputPaths(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);
        boolean success = job.waitForCompletion(true);

        job.getCounters().findCounter(Counters.ARTICLE_COUNTER).getValue();
        return success ? 0 : 1;
    }
}
