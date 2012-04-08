package ru.vasily.shad.parallel.tasks.philosophers;

import java.util.Random;

public abstract class Philosopher<ForkType> implements Runnable
{

    final int position;
    final ForkType left;
    final ForkType right;
    int eatCount = 0;
    long waitTime = 0;
    long startWait;
    volatile boolean stopFlag = false;
    Random rnd = new Random();

    public Philosopher(int position, ForkType left, ForkType right)
    {
        this.position = position;
        this.left = left;
        this.right = right;
    }

    public void eat()
    {
        waitTime += System.currentTimeMillis() - startWait;
//        System.out.println("[Philosopher " + position + "] is eating");
        try
        {
            Thread.sleep(rnd.nextInt(100));
        }
        catch (InterruptedException e)
        {
            e.printStackTrace();
        }
        eatCount++;
//        System.out.println("[Philosopher " + position + "] finished eating");
    }

    public void think()
    {
//        System.out.println("[Philosopher " + position + "] is thinking");
        try
        {
            Thread.sleep(rnd.nextInt(100));
        }
        catch (InterruptedException e)
        {
            e.printStackTrace();
        }
//        System.out.println("[Philosopher " + position + "] is hungry");
        startWait = System.currentTimeMillis();
    }

    public void stop()
    {
        stopFlag = true;
    }

    @Override
    public void run()
    {
        while (!stopFlag)
        {
            think();
            getForksAndEat();
        }
//        System.out.println("[Philosopher " + position + "] stopped");
    }

    protected abstract void getForksAndEat();

}
