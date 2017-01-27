package no.systek.dataflow.steps;

import java.util.LinkedList;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import no.systek.dataflow.PriorityTaskQueue;
import no.systek.dataflow.Step;

/**
 * Conditional step for 2-way step dependencies which can route dynamically depending on the conditional
 * output
 * <p>
 * See CappuccinoTest
 */
@SuppressWarnings("WeakerAccess")
public abstract class ConditionalStep<I, O> extends Step<I, O> {

    private final List<Step<O, ?>> falseChildren = new LinkedList<>();

    public ConditionalStep(String name, int maxParallelExecution) {
        super(name, maxParallelExecution);
    }

    protected abstract void run(I input, BiConsumer<Boolean, O> onResult);

    @Override
    protected final void run(I input, Consumer<O> onResult) {
        run(input, (condition, result) -> onResult.accept((O) new ConditionalResult<>(condition, result)));
    }

    @Override
    protected final void onOutputAvailable(O output, PriorityTaskQueue pq) {
        ConditionalResult<O> r = (ConditionalResult<O>) output;
        List<Step<O, ?>> next = r.condition ? getChildren() : falseChildren;
        if (next.isEmpty()) {
            onResult.accept(r.output);
        } else {
            next.forEach(s -> s.post(r.output, pq));
        }
    }

    @Override
    public DependencyCreator<Object, O> output() {
        throw new IllegalArgumentException("Cannot use this on ConditionalStep");
    }

    public DependencyCreator<Object, O> ifTrue() {
        return new DependencyCreator<Object, O>((Step<Object, O>) this) {
            @Override
            public void link(Step<O, ?> child) {
                getChildren().add(child);
            }
        };
    }

    public DependencyCreator<Object, O> ifFalse() {
        return new DependencyCreator<Object, O>((Step<Object, O>) this) {
            @Override
            public void link(Step<O, ?> child) {
                falseChildren.add(child);
            }
        };
    }

    private static class ConditionalResult<O> {
        private final boolean condition;
        private final O output;

        ConditionalResult(boolean condition, O output) {
            this.condition = condition;
            this.output = output;
        }
    }
}
