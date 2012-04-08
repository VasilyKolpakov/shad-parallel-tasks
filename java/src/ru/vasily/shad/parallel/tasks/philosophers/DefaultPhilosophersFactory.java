package ru.vasily.shad.parallel.tasks.philosophers;

import java.util.ArrayList;
import java.util.List;

public class DefaultPhilosophersFactory implements PhilosophersFactory
{

    @Override
    public List<Philosopher<?>> getPhilosophers(int numberOfPhilosophers)
    {
        List<Philosopher<?>> philosophers = new ArrayList<Philosopher<?>>();
        Fork last = new Fork();
        Fork left = last;
        for (int i = 0; i < numberOfPhilosophers; i++)
        {
            Fork right = (i == numberOfPhilosophers - 1) ? last : new Fork();
            philosophers.add(new DefaultPhilosopher(i, left, right));
            left = right;
        }
        return philosophers;
    }
}
