package ru.vasily.shad.parallel.tasks.philosophers;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

public class Run
{

    private static class TestResult
    {
        public final double meanEatingTimes;
        public final double meanEatingTimesDispersion;
        private final double meanWaitingTime;
        private final double meanWaitingDispersion;

        private TestResult(double meanEatingTimes, double meanEatingTimesDispersion, double meanWaitingTime, double meanWaitingDispersion)
        {
            this.meanEatingTimes = meanEatingTimes;
            this.meanEatingTimesDispersion = meanEatingTimesDispersion;
            this.meanWaitingTime = meanWaitingTime;
            this.meanWaitingDispersion = meanWaitingDispersion;
        }
    }

    public static void main(String[] args) throws Exception
    {
//        for (int count = 50; count < 4060; count += 500)
//        {
        int count = 5000;
            TestResult result = doTest(count);
            System.out.println("number of philosophers = " + count);
            System.out.println("mean eating times = " + result.meanEatingTimes);
            System.out.println("eating times dispersion sqrt = " + result.meanEatingTimesDispersion);
            System.out.println("mean waiting time = " + result.meanWaitingTime);
            System.out.println("eating waiting time dispersion sqrt = " + result.meanWaitingDispersion);
//        }
//        for (Philosopher phil : philosophers)
//        {
//            System.out.println("[Philosopher " + phil.position + "] ate "
//                                       + phil.eatCount + " times and waited " + phil.waitTime + " ms");
//        }
    }

    private static TestResult doTest(int count) throws InterruptedException, ExecutionException
    {
        PhilosophersFactory philosophersFactory = new MyPhilosophersFactory();
        List<Philosopher<?>> philosophers = philosophersFactory.getPhilosophers(count);
        ExecutorService executor = Executors.newCachedThreadPool();
        List<Future> futures = submitRunnables(philosophers, executor);
        Thread.sleep(60000);
        for (Philosopher phil : philosophers)
        {
            phil.stop();
        }
        waitForPhilosophers(futures);
        executor.shutdown();
        return new TestResult(
                getMeanEatingTimes(philosophers),
                Math.sqrt(getMeanEatingDispersion(philosophers)),
                getMeanWaitingTime(philosophers),
                Math.sqrt(getMeanWaitingTimeDispersion(philosophers))
        );
    }

    private static double getMeanEatingDispersion(List<Philosopher<?>> philosophers)
    {
        double mean = getMeanEatingTimes(philosophers);
        double sum = 0;
        for (Philosopher phil : philosophers)
        {
            double diff = phil.eatCount - mean;
            sum += diff * diff;
        }
        return sum / philosophers.size();
    }

    private static double getMeanEatingTimes(List<Philosopher<?>> philosophers)
    {
        double sum = 0;
        for (Philosopher phil : philosophers)
        {
            sum += phil.eatCount;
        }
        return sum / philosophers.size();
    }

    private static double getMeanWaitingTimeDispersion(List<Philosopher<?>> philosophers)
    {
        double mean = getMeanWaitingTime(philosophers);
        double sum = 0;
        for (Philosopher phil : philosophers)
        {
            double diff = phil.waitTime - mean;
            sum += diff * diff;
        }
        return sum / philosophers.size();
    }

    private static double getMeanWaitingTime(List<Philosopher<?>> philosophers)
    {
        double sum = 0;
        for (Philosopher phil : philosophers)
        {
            sum += phil.waitTime;
        }
        return sum / philosophers.size();
    }


    private static List<Future> submitRunnables(List<Philosopher<?>> philosophers, ExecutorService executor)
    {
        List<Future> futures = new ArrayList<Future>();
        for (Runnable philosopher : philosophers)
        {
            Future<?> future = executor.submit(philosopher);
            futures.add(future);
        }
        return futures;
    }

    private static void waitForPhilosophers(List<Future> futures) throws InterruptedException, ExecutionException
    {
        for (Future future : futures)
        {
            future.get();
        }
    }


}
