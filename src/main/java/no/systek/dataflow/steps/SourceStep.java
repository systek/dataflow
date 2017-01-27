package no.systek.dataflow.steps;

import java.util.function.Consumer;

import no.systek.dataflow.Step;

/**
 * Simple step which ignores the input and produces output(s) independent from the input
 */
public abstract class SourceStep<O> extends Step<Object, O> {

    public SourceStep(String name, int maxParallelExecution) {
        super(name, maxParallelExecution);
    }

    @Override
    protected void run(Object input, Consumer<O> onResult) {
        onResult.accept(get());
    }

    protected abstract O get();
}
