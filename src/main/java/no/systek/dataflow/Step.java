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
@SuppressWarnings("WeakerAccess")
public abstract class Step<I, O> {

    private final String name;
    private final int maxParallelExecution;
    private final List<Step<?, I>> parents = new LinkedList<>();
    private final List<Step<O, ?>> children = new LinkedList<>();
    private final Queue<I> msgBox = new ConcurrentLinkedQueue<>();
    private final AtomicInteger scheduledJobs = new AtomicInteger();
    private final AtomicInteger lock = new AtomicInteger();
    private volatile int graphDepth;
    protected volatile Consumer<O> onResult;

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
        Consumer<O> onResult,
        long timeout,
        TimeUnit unit) {

        if (!children.isEmpty()) {
            throw new RuntimeException("This step has children; please start executing at the tail of the graph");
        }
        this.onResult = onResult;

        // walk the step graph: configure the graph depth on each step and find the root steps
        HashSet<Step<Object, ?>> roots = new HashSet<>();
        configureTreeAndFindRoots(new HashSet<>(), roots);

        // start execution by scheduling tasks for all roots
        roots.forEach(rootStep -> rootStep.post(input, taskQueue));

        return taskQueue.executeTasksAndAwaitDone(executorService, exceptionListener, timeout, unit);
    }

    public void dependsOn(DependencyCreator<Object, I> dependency) {
        addParent(dependency.step);
        dependency.link(this);
    }

    public DependencyCreator<Object, O> output() {
        return new DependencyCreator<>((Step<Object, O>) this);
    }

    public void post(I input, PriorityTaskQueue taskQueue) {
        if (input != null) {
            msgBox.offer(input);
        }

        // try to schedule a new task on the thread pool which handles this new input
        tryScheduleNextJob(taskQueue);
    }

    protected abstract void run(I input, Consumer<O> onResult);

    protected void afterRun(PriorityTaskQueue taskQueue) {
    }

    protected int configureTreeAndFindRoots(HashSet<Step<?, ?>> visited, HashSet<Step<Object, ?>> roots) {
        try {
            if (!visited.add(this)) {
                return this.graphDepth;
            }

            int myDepth = 1;

            if (parents.isEmpty() || (parents.size() == 1 && visited.contains(parents.get(0)))) {
                roots.add((Step<Object, ?>) this);
            } else {
                for (Step<?, ?> parent : parents) {
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

    private void tryScheduleNextJob(PriorityTaskQueue taskQueue) {
        // only one thread at a time here
        if (lock.getAndIncrement() == 0) {
            try {
                while (msgBox.peek() != null && scheduledJobs.get() < maxParallelExecution) {
                    scheduledJobs.incrementAndGet();
                    I input = msgBox.poll();
                    taskQueue.addTask(PriorityTaskQueue.HIGHEST_PRIORITY, pq -> {
                        try {
                            run(input, output -> onOutputAvailable(output, pq));
                            afterRun(pq);
                        } finally {
                            scheduledJobs.decrementAndGet();
                            tryScheduleNextJob(pq);
                        }
                    });
                }
            } finally {
                if (lock.getAndSet(0) != 1) {
                    // another thread tried to enter this block while we had the lock, re-run it in case new messages
                    // have arrived
                    taskQueue.addTask(PriorityTaskQueue.HIGHEST_PRIORITY, this::tryScheduleNextJob);
                }
            }
        }
    }

    protected void onOutputAvailable(O output, PriorityTaskQueue pq) {
        if (children.isEmpty()) {
            onResult.accept(output);
        } else {
            children.forEach(s -> s.post(output, pq));
        }
    }

    protected void addParent(Step<?, I> parent) {
        this.parents.add(parent);
    }

    protected List<Step<O, ?>> getChildren() {
        return children;
    }

    protected Integer getGraphDepth() {
        return graphDepth;
    }

    protected static class DependencyCreator<I, O> {
        public final Step<I, O> step;

        public DependencyCreator(Step<I, O> step) {
            this.step = step;
        }

        public void link(Step<O, ?> child) {
            step.children.add(child);
        }
    }
}
