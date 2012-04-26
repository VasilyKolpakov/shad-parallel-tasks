package ru.vasily.shad.parallel.task3.index;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class ArticleNameAndFrequency implements Writable
{
    private String articleName;
    private double frequency;

    public void set(String articleName, double frequency)
    {
        this.articleName = articleName;
        this.frequency = frequency;
    }

    public double getFrequency()
    {
        return frequency;
    }

    public String getArticleName()
    {
        return articleName;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException
    {
        dataOutput.writeUTF(articleName);
        dataOutput.writeDouble(frequency);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException
    {
        articleName = dataInput.readUTF();
        frequency = dataInput.readDouble();
    }

    @Override
    public String toString()
    {
        return articleName + '\t' + frequency;
    }
}
