package ru.vasily.shad.parallel.tasks.philosophers;

import java.util.List;

public interface PhilosophersFactory
{
    List<Philosopher<?>> getPhilosophers(int numberOfPhilosophers);
}
