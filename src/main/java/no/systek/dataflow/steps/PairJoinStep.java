package no.systek.dataflow.steps;

import java.util.LinkedList;
import java.util.Queue;
import java.util.function.Consumer;

import no.systek.dataflow.PriorityTaskQueue;
import no.systek.dataflow.Step;

/**
 * Collects and sorts inputs into two internal queues and produces one joined output as soon as one of
 * each input type is available
 */
public abstract class PairJoinStep<Ileft, Iright, O> extends Step<Object, O> {

    private final Object CLEANUP = new Object();
    private final Queue<Ileft> left;
    private final Queue<Iright> right;
    private boolean cleanupScheduled;

    protected PairJoinStep(String name) {
        super(name, 1);
        this.left = new LinkedList<>();
        this.right = new LinkedList<>();
    }

    protected abstract O join(Ileft left, Iright right);

    protected abstract boolean isLeft(Object input);

    @Override
    protected void run(Object input, Consumer<O> onResult) {
        if (CLEANUP.equals(input)) {
            cleanupScheduled = false;
            if (!left.isEmpty() || !right.isEmpty()) {
                throw new RuntimeException("Joiner step has unsatisfied dependencies, something went wrong");
            }
            return;
        }

        if (isLeft(input)) {
            left.add((Ileft) input);
        } else {
            right.add((Iright) input);
        }

        if (!left.isEmpty() && !right.isEmpty()) {
            onResult.accept(join(left.poll(), right.poll()));
        }
    }

    @Override
    protected void afterRun(PriorityTaskQueue taskQueue) {
        if ((!left.isEmpty() || !right.isEmpty()) && !cleanupScheduled) {
            cleanupScheduled = true;
            taskQueue.addTask(getGraphDepth(), tq -> post(CLEANUP, tq));
        }
    }

    public void dependsOnLeft(DependencyCreator<Object, Ileft> left) {
        addParent((Step<?, Object>) left.step);
        left.link((Step<Ileft, ?>) this);
    }

    public void dependsOnRight(DependencyCreator<Object, Iright> right) {
        addParent((Step<?, Object>) right.step);
        right.link((Step<Iright, ?>) this);
    }

    @Override
    public void dependsOn(DependencyCreator<Object, Object> dependency) {
        throw new IllegalArgumentException(
            "Cannot use dependsOn() on PairJoinStep, use dependsOnLeft/dependsOnRight instead");
    }
}