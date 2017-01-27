package no.systek.dataflow.steps;

import java.util.List;
import java.util.function.Consumer;

import no.systek.dataflow.Step;

public abstract class ListStep<I, O> extends Step<List<I>, O> {

    protected ListStep(String name) {
        super(name, Integer.MAX_VALUE);
    }

    @Override
    protected void run(List<I> input, Consumer<O> onResult) {
        //noinspection unchecked
        execute(input).forEach(onResult);
    }

    protected abstract List<O> execute(List<I> in);
}
