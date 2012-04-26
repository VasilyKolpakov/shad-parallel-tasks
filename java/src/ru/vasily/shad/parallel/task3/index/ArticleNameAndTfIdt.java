package ru.vasily.shad.parallel.task3.index;

import java.util.Comparator;

public class ArticleNameAndTfIdt
{
    public static Comparator<ArticleNameAndTfIdt> BY_TFIDF_DESC = new Comparator<ArticleNameAndTfIdt>()
    {
        @Override
        public int compare(ArticleNameAndTfIdt o1, ArticleNameAndTfIdt o2)
        {
            return -Double.compare(o1.tfIdf, o2.tfIdf);
        }
    };

    public final String articleName;
    public final double tfIdf;

    public ArticleNameAndTfIdt(String articleName, double tfIdf)
    {
        this.articleName = articleName;
        this.tfIdf = tfIdf;
    }
}
