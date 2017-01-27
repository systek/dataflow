package no.systek.dataflow.steps;

import java.util.LinkedList;
import java.util.List;
import java.util.function.Consumer;

import no.systek.dataflow.PriorityTaskQueue;
import no.systek.dataflow.Step;

public class CollectorStep<T> extends Step<T, List<T>> {
    private final T CLEANUP = (T) "CLEANUP";

    private final List<T> items;
    private final int bufferSize;
    private boolean scheduledCleanup;

    public CollectorStep(String name, int bufferSize) {
        super(name, 1);
        this.bufferSize = bufferSize;
        this.items = new LinkedList<>();
    }

    @Override
    protected void run(T input, Consumer<List<T>> onResult) {
        if (CLEANUP.equals(input)) {
            scheduledCleanup = false;
            pushItems(onResult);
        } else {
            if (items.size() >= bufferSize) {
                pushItems(onResult);
            }
            items.add(input);
        }
    }

    @Override
    protected void afterRun(PriorityTaskQueue priorityTaskQueue) {
        // schedule a cleanup tasks at a lower priority such that if there are no more
        // steps executing (thus waiting for some input), then this step should run using
        // the items it got so far.

        // do this only once, the the first item is scheduled
        if (!items.isEmpty() && !scheduledCleanup) {
            scheduledCleanup = true;
            priorityTaskQueue.addTask(getGraphDepth(), pq2 -> post(CLEANUP, pq2));
        }
    }

    private void pushItems(Consumer<List<T>> onResult) {
        List<T> items = new LinkedList<>(this.items);
        this.items.clear();
        onResult.accept(items);
    }
}
