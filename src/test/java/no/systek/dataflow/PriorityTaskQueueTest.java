package no.systek.dataflow;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import org.junit.Test;

public class PriorityTaskQueueTest {

    @Test
    public void testParallelExecutionAtPriority1() {
        final int parallelTasks = 10;
        PriorityTaskQueue pq = new PriorityTaskQueue(parallelTasks, () -> null, s -> {
        });

        AtomicInteger successCounter = new AtomicInteger();
        CountDownLatch c = new CountDownLatch(parallelTasks);

        for (int i = 0; i < parallelTasks; i++) {
            pq.addTask(2, e -> {
                c.countDown();
                try {
                    if (c.await(2, TimeUnit.SECONDS)) {
                        successCounter.incrementAndGet();
                    }
                } catch (InterruptedException ignored) {
                }
            });
        }

        ExecutorService executorService = Executors.newCachedThreadPool();
        Queue<Exception> exceptions = new LinkedList<>();
        assertThat(pq.executeTasksAndAwaitDone(executorService, exceptions::offer, 1, TimeUnit.SECONDS),
            is(true));
        assertThat(exceptions.size(), is(0));
        assertThat(successCounter.get(), is(parallelTasks));
        executorService.shutdown();
    }

    @Test
    public void testAllTasksAtPriority1AreDoneFirst() {
        final int parallelTasks = 10;
        PriorityTaskQueue pq = new PriorityTaskQueue(parallelTasks, () -> null, s -> {
        });

        final AtomicInteger priority1Started = new AtomicInteger(parallelTasks);
        final AtomicInteger errorCounter = new AtomicInteger();
        Consumer<PriorityTaskQueue> prio1Task = e -> priority1Started.decrementAndGet();
        Consumer<PriorityTaskQueue> prio2Task = e -> {
            if (priority1Started.get() == parallelTasks) {
                errorCounter.incrementAndGet();
            }
        };

        for (int i = 0; i < parallelTasks; i++) {
            pq.addTask(2, prio2Task);
            pq.addTask(1, prio1Task);
        }

        ExecutorService executorService = Executors.newCachedThreadPool();
        Queue<Exception> exceptions = new LinkedList<>();
        assertThat(
            pq.executeTasksAndAwaitDone(executorService, exceptions::offer, 1, TimeUnit.SECONDS),
            is(true));
        assertThat(exceptions.size(), is(0));
        assertThat(errorCounter.get(), is(0));
        executorService.shutdown();
    }

    @Test
    public void testMovingBackToHigherPriority() {
        final int parallelTasks = 10;
        PriorityTaskQueue pq = new PriorityTaskQueue(parallelTasks, () -> null, s -> {
        });

        AtomicBoolean test = new AtomicBoolean(false);
        AtomicInteger errors = new AtomicInteger();
        AtomicInteger doneCounter = new AtomicInteger();
        pq.addTask(10, q1 -> {
            q1.addTask(9, q2 -> {
                q2.addTask(7, q3 -> {
                    if (!test.compareAndSet(false, true)) {
                        errors.incrementAndGet();
                    }
                    doneCounter.incrementAndGet();
                });
                q2.addTask(8, q3 -> {
                    if (!test.compareAndSet(true, false)) {
                        errors.incrementAndGet();
                    }
                    doneCounter.incrementAndGet();
                });
                doneCounter.incrementAndGet();
            });
            doneCounter.incrementAndGet();
        });

        ExecutorService executorService = Executors.newCachedThreadPool();
        Queue<Exception> exceptions = new LinkedList<>();
        assertThat(
            pq.executeTasksAndAwaitDone(executorService, exceptions::offer, 1, TimeUnit.SECONDS),
            is(true));
        assertThat(exceptions.size(), is(0));
        assertThat(errors.get(), is(0));
        assertThat(doneCounter.get(), is(4));
        executorService.shutdown();
    }

    @Test
    public void korrelasjonsIdIsPreserved() {
        final int parallelTasks = 10;

        String korrelatjonsId = "TEST123";
        ThreadLocal<String> korrelasjonsId = new ThreadLocal<>();
        korrelasjonsId.set(korrelatjonsId);
        PriorityTaskQueue pq = new PriorityTaskQueue(parallelTasks, korrelasjonsId::get, korrelasjonsId::set);

        AtomicReference<String> capcturedKorrelatjonsId = new AtomicReference<>();
        pq.addTask(1, q -> capcturedKorrelatjonsId.set(korrelasjonsId.get()));

        ExecutorService executorService = Executors.newCachedThreadPool();
        Queue<Exception> exceptions = new LinkedList<>();
        assertThat(
            pq.executeTasksAndAwaitDone(executorService, exceptions::offer, 1, TimeUnit.SECONDS),
            is(true));
        assertThat(exceptions.size(), is(0));
        assertThat(capcturedKorrelatjonsId.get(), is(korrelatjonsId));
        executorService.shutdown();

    }

    @Test
    public void testTimeout() {
        PriorityTaskQueue pq = new PriorityTaskQueue(10, () -> null, s -> {
        });
        pq.addTask(2, q -> {
            while (true) {
                try {
                    Thread.sleep(2000);
                    break;
                } catch (InterruptedException ignored) {
                }
            }
        });
        ExecutorService executorService = Executors.newCachedThreadPool();
        Queue<Exception> exceptions = new LinkedList<>();
        assertThat(
            pq.executeTasksAndAwaitDone(executorService, exceptions::offer, 1, TimeUnit.SECONDS),
            is(false));
        assertThat(exceptions.size(), is(0));
        executorService.shutdown();

    }

}