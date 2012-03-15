package ru.vasily.shad.parallel.tasks.philosophers;


public class DefaultPhilosopher extends Philosopher<Fork>
{
    public DefaultPhilosopher(int position, Fork left, Fork right)
    {
        super(position, left, right);
    }


    protected void getForksAndEat()
    {
        synchronized (left)
        {
            System.out.println("[Philosopher " + position + "] took left fork");
            synchronized (right)
            {
                System.out.println("[Philosopher " + position + "] took right fork");
                eat();
            }
        }
    }

}