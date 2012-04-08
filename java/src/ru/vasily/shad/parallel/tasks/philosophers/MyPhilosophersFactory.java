package ru.vasily.shad.parallel.tasks.philosophers;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class MyPhilosophersFactory implements PhilosophersFactory
{
    @Override
    public List<Philosopher<?>> getPhilosophers(int numberOfPhilosophers)
    {

        List<Philosopher<?>> philosophers = new ArrayList<Philosopher<?>>();
        MyFork last = new MyFork();
        MyFork left = last;
        for (int i = 0; i < numberOfPhilosophers; i++)
        {
            MyFork right = (i == numberOfPhilosophers - 1) ? last : new MyFork();
            philosophers.add(new MyPhilosopher(i, left, right));
            left = right;
        }
        return philosophers;
    }

    private class MyPhilosopher extends Philosopher<MyFork>
    {
        public MyPhilosopher(int position, MyFork left, MyFork right)
        {
            super(position, left, right);
        }

        @Override
        protected void getForksAndEat()
        {
            try
            {
                boolean gotLock = false;
                while (!gotLock)
                {
                    if (left.getLock().tryLock())
                    {
                        if (right.getLock().tryLock(rnd.nextInt(10), TimeUnit.MILLISECONDS))
                        {
                            gotLock = true;
                        }
                        else
                        {
                            left.getLock().unlock();
                        }
                    }
                    Thread.sleep(5);
                }
                eat();
            }
            catch (InterruptedException e)
            {
                e.printStackTrace();
            }
            finally
            {
                left.getLock().unlock();
                right.getLock().unlock();
            }
        }

    }

    private class MyFork
    {
        private Lock lock = new ReentrantLock();

        Lock getLock()
        {
            return lock;
        }
    }
}
