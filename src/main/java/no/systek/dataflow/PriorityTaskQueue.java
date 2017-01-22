package no.systek.dataflow;

import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * Schedules tasks ordered by priority
 */
public class PriorityTaskQueue {
    public static final int HIGHEST_PRIORITY = 1;

    private final Lock lock;
    private final Condition taskCompleted;
    private final int maxParallelTasks;
    private final Supplier<String> correlationIdProvider;
    private final Consumer<String> correlationIdSetter;

    // guarded by "lock"
    private final AtomicInteger runningTasks = new AtomicInteger(0);
    private final List<Queue<Consumer<PriorityTaskQueue>>> queues;

    public PriorityTaskQueue(int maxParallelTasks,
                             Supplier<String> correlationIdProvider,
                             Consumer<String> correlationIdSetter) {

        this.lock = new ReentrantLock();
        this.taskCompleted = this.lock.newCondition();
        this.maxParallelTasks = maxParallelTasks;
        this.queues = new LinkedList<>();
        this.correlationIdProvider = correlationIdProvider;
        this.correlationIdSetter = correlationIdSetter;
    }

    public void addTask(int priority, Consumer<PriorityTaskQueue> task) {
        if (priority < HIGHEST_PRIORITY) {
            throw new RuntimeException("Priority cannot be lower than 1");
        }
        locked(() -> {
            while (queues.size() < priority) {
                queues.add(new LinkedBlockingQueue<>());
            }
            queues.get(priority - 1).offer(task);
        });
    }

    /**
     * Executes all queued tasks until all done. It tries to complete all tasks at the highest
     * priority first before moving to the next priority. If a new task got scheduled at a higher
     * priority in the mean time, it moves back to the higher priority
     *
     * @return true if all queues has been done, false if timeout was reached
     */
    public boolean executeTasksAndAwaitDone(
            ExecutorService executorService,
            Consumer<Exception> exceptionListener,
            long timeout,
            TimeUnit unit) {

        final long deadLine = System.currentTimeMillis() + unit.toMillis(timeout);

        return locked(() -> {

            int currentPriority = 0;
            boolean foundTasksAtPriority = false;
            while (true) {

                if (System.currentTimeMillis() > deadLine) {
                    return false;
                }
                if (currentPriority >= queues.size()) {
                    return true;
                }

                Queue<Consumer<PriorityTaskQueue>> tasksAtCurrentPriority = queues.get(currentPriority);

                if (!tasksAtCurrentPriority.isEmpty()) {
                    foundTasksAtPriority = true;
                    if (tryScheduleTask(executorService, tasksAtCurrentPriority.peek(), exceptionListener)) {
                        tasksAtCurrentPriority.poll();
                        continue;
                    }
                }

                if (runningTasks.get() > 0) {
                    try {
                        taskCompleted.await(Math.max(1, deadLine - System.currentTimeMillis()), TimeUnit.MILLISECONDS);
                    } catch (InterruptedException ignored) {
                    }
                    continue;
                }

                currentPriority = !foundTasksAtPriority ? currentPriority + 1 : 0;
                foundTasksAtPriority = false;
            }
        });

    }

    /**
     * Submit a task to the thread pool
     *
     * @return true is the task was submitted, or false if max parallel tasks has been reached
     */
    private boolean tryScheduleTask(
            ExecutorService executorService,
            Consumer<PriorityTaskQueue> task,
            Consumer<Exception> exceptionListener) {

        if (runningTasks.incrementAndGet() <= maxParallelTasks) {
            executorService.submit(ContextSwitcher.wrap(() -> {
                try {
                    task.accept(this);
                } catch (Exception e) {
                    exceptionListener.accept(e);
                } finally {
                    locked(() -> {
                        runningTasks.decrementAndGet();
                        taskCompleted.signalAll();
                    });
                }
            }, correlationIdProvider, correlationIdSetter));
            return true;
        } else {
            // give it back
            runningTasks.decrementAndGet();
            return false;
        }
    }

    private void locked(Runnable r) {
        locked(() -> {
            r.run();
            return false;
        });
    }

    private <T> T locked(Callable<T> r) {
        lock.lock();
        try {
            try {
                return r.call();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        } finally {
            lock.unlock();
        }
    }
}
