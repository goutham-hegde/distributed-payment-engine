package com.dpe.account.support;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * Runs N tasks with a starting gun, so they contend for real instead of trickling in.
 *
 * <p>Without the latch, thread 1 typically finishes before thread 8 is scheduled, the lock is
 * never contended, and a completely broken implementation passes. The latch is what makes these
 * tests capable of failing.
 */
public final class Concurrently {

    private Concurrently() {
    }

    /**
     * @return one outcome per task, in submission order: either a value or the exception thrown.
     */
    public static <T> List<Outcome<T>> run(int threads, Callable<T> task) {
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch startingGun = new CountDownLatch(1);
        List<Future<T>> futures = new ArrayList<>(threads);
        try {
            for (int i = 0; i < threads; i++) {
                futures.add(pool.submit(() -> {
                    startingGun.await();
                    return task.call();
                }));
            }
            startingGun.countDown();
            return collect(futures);
        } finally {
            pool.shutdown();
            awaitTermination(pool);
        }
    }

    /** Runs a fixed list of distinct tasks concurrently. */
    public static <T> List<Outcome<T>> runAll(List<Callable<T>> tasks) {
        ExecutorService pool = Executors.newFixedThreadPool(tasks.size());
        CountDownLatch startingGun = new CountDownLatch(1);
        List<Future<T>> futures = new ArrayList<>(tasks.size());
        try {
            for (Callable<T> task : tasks) {
                futures.add(pool.submit(() -> {
                    startingGun.await();
                    return task.call();
                }));
            }
            startingGun.countDown();
            return collect(futures);
        } finally {
            pool.shutdown();
            awaitTermination(pool);
        }
    }

    private static <T> List<Outcome<T>> collect(List<Future<T>> futures) {
        List<Outcome<T>> outcomes = new ArrayList<>(futures.size());
        for (Future<T> future : futures) {
            try {
                outcomes.add(new Outcome<>(future.get(60, TimeUnit.SECONDS), null));
            } catch (Exception e) {
                Throwable cause = e.getCause() == null ? e : e.getCause();
                outcomes.add(new Outcome<>(null, cause));
            }
        }
        return outcomes;
    }

    private static void awaitTermination(ExecutorService pool) {
        try {
            if (!pool.awaitTermination(60, TimeUnit.SECONDS)) {
                pool.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            pool.shutdownNow();
        }
    }

    /** A task either produced a value or threw. */
    public record Outcome<T>(T value, Throwable failure) {

        public boolean succeeded() {
            return failure == null;
        }

        public boolean failedWith(Class<? extends Throwable> type) {
            return failure != null && type.isInstance(failure);
        }
    }
}
