package com.dehnes.dataflow;

import java.util.LinkedList;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * Conditional step for 2-way step dependencies which can route dynamically depending on the conditional
 * output
 * <p>
 * See CappuccinoTest
 */
public abstract class ConditionalStep extends Step {

    private final List<Step> falseChildren = new LinkedList<>();

    public ConditionalStep(String name, int maxParallelExecution) {
        super(name, maxParallelExecution);
    }

    protected abstract void run(Object input, BiConsumer<Boolean, Object> onResult);

    @Override
    protected final void run(Object input, Consumer<Object> onResult) {
        run(input, (condition, result) -> onResult.accept(new ConditionalResult(condition, result)));
    }

    @Override
    protected final void onOutputAvailable(Object output, PriorityTaskQueue pq, Consumer<Object> onFinalResult) {
        ConditionalResult r = (ConditionalResult) output;
        List<Step> next = r.condition ? getChildren() : falseChildren;
        if (next.isEmpty()) {
            onFinalResult.accept(r.result);
        } else {
            next.forEach(s -> s.post(r.result, pq, onFinalResult));
        }
    }

    @Override
    public DependencyCreator output() {
        throw new IllegalArgumentException("Cannot use this on ConditionalStep");
    }

    public DependencyCreator ifTrue() {
        return new DependencyCreator(this) {
            @Override
            void create(Step dependency) {
                getChildren().add(dependency);
            }
        };
    }

    public DependencyCreator ifFalse() {
        return new DependencyCreator(this) {
            @Override
            void create(Step dependency) {
                falseChildren.add(dependency);
            }
        };
    }

    private static class ConditionalResult {
        private final boolean condition;
        private final Object result;

        public ConditionalResult(boolean condition, Object result) {
            this.condition = condition;
            this.result = result;
        }
    }
}
