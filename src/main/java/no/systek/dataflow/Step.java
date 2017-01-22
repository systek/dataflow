package no.systek.dataflow;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

/**
 * Similar to "actors", a step is a piece of work which is executed as some input arrives and can produce
 * one or more outputs.
 * <p>
 * The config parameter "maxParallelExecution" controls how many times this Step can be started in parallel
 * to handle inbound events concurrently. Set it to 1 to disallow concurrent execution, in which case internal state
 * is protected from concurrent access.
 * <p>
 * Steps can be chained together to (complex) graphs by defining dependencies between then, including loop scenarios.
 * <p>
 * See CappuccinoTest
 * <p>
 */
public abstract class Step {

    private final String name;
    private final int maxParallelExecution;
    private final List<Step> parents = new LinkedList<>();
    private final List<Step> children = new LinkedList<>();
    private final Queue<Object> msgBox = new ConcurrentLinkedQueue<>();
    private final AtomicInteger scheduledJobs = new AtomicInteger();
    private final AtomicInteger lock = new AtomicInteger();
    private volatile int graphDepth;

    public Step(int maxParallelExecution) {
        this(null, maxParallelExecution);
    }

    public Step(String name, int maxParallelExecution) {
        this.name = name == null ? this.getClass().getSimpleName() : name;
        this.maxParallelExecution = maxParallelExecution;
    }

    public String getName() {
        return name;
    }

    public boolean executeTasksAndAwaitDone(
            PriorityTaskQueue taskQueue,
            ExecutorService executorService,
            Consumer<Exception> exceptionListener,
            Object input,
            Consumer<Object> onResult,
            long timeout,
            TimeUnit unit) {

        // walk the step graph: configure the graph depth on each step and find the root steps
        HashSet<Step> roots = new HashSet<>();
        configureTreeAndFindRoots(new HashSet<>(), roots);

        // start execution by scheduling tasks for all roots
        roots.forEach(rootStep -> rootStep.post(input, taskQueue, onResult));

        return taskQueue.executeTasksAndAwaitDone(executorService, exceptionListener, timeout, unit);
    }

    public void dependsOn(DependencyCreator dependency) {
        addParent(dependency.step);
        dependency.create(this);
    }

    public DependencyCreator output() {
        return new DependencyCreator(this) {
            @Override
            void create(Step dependency) {
                children.add(dependency);
            }
        };
    }

    public void post(Object input, PriorityTaskQueue taskQueue, Consumer<Object> onFinalResult) {
        if (input != null) {
            msgBox.offer(input);
        }

        // try to schedule a new task on the thread pool which handles this new input
        tryScheduleNextJob(taskQueue, onFinalResult);
    }

    protected abstract void run(Object input, Consumer<Object> onResult);

    protected void afterRun(PriorityTaskQueue priorityTaskQueue, Consumer<Object> onFinalResult) {
    }

    protected int configureTreeAndFindRoots(HashSet<Step> visited, HashSet<Step> roots) {
        try {
            if (!visited.add(this)) {
                return this.graphDepth;
            }

            int myDepth = 1;

            if (parents.isEmpty() || (parents.size() == 1 && visited.contains(parents.get(0)))) {
                roots.add(this);
            } else {
                for (Step parent : parents) {
                    int parentLevel = parent.configureTreeAndFindRoots(visited, roots);
                    if (parentLevel > myDepth) {
                        myDepth = parentLevel;
                    }
                }
                myDepth++;
            }

            this.graphDepth = myDepth;

            return myDepth;
        } finally {
            visited.remove(this);
        }
    }

    private void tryScheduleNextJob(PriorityTaskQueue taskQueue, Consumer<Object> onFinalResult) {
        // only one thread at a time here
        if (lock.getAndIncrement() == 0) {
            try {
                while (msgBox.peek() != null && scheduledJobs.get() < maxParallelExecution) {
                    scheduledJobs.incrementAndGet();
                    Object input = msgBox.poll();
                    taskQueue.addTask(PriorityTaskQueue.HIGHEST_PRIORITY, pq -> {
                        try {
                            run(input, output -> onOutputAvailable(output, pq, onFinalResult));
                            afterRun(pq, onFinalResult);
                        } finally {
                            scheduledJobs.decrementAndGet();
                            tryScheduleNextJob(pq, onFinalResult);
                        }
                    });
                }
            } finally {
                if (lock.getAndSet(0) != 1) {
                    // another thread tried to enter this block while we had the lock, re-run it in case new messages
                    // have arrived
                    taskQueue.addTask(PriorityTaskQueue.HIGHEST_PRIORITY, pq2 -> tryScheduleNextJob(pq2, onFinalResult));
                }
            }
        }
    }

    protected void onOutputAvailable(Object output, PriorityTaskQueue pq, Consumer<Object> onFinalResult) {
        if (children.isEmpty()) {
            onFinalResult.accept(output);
        } else {
            children.forEach(s -> s.post(output, pq, onFinalResult));
        }
    }

    protected void addParent(Step parent) {
        this.parents.add(parent);
    }

    protected List<Step> getChildren() {
        return children;
    }

    protected Integer getGraphDepth() {
        return graphDepth;
    }

    protected abstract static class DependencyCreator {
        private final Step step;

        public DependencyCreator(Step step) {
            this.step = step;
        }

        abstract void create(Step dependency);
    }
}
