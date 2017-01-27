package no.systek.dataflow;

import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

import no.systek.dataflow.steps.CollectorStep;
import no.systek.dataflow.steps.ConditionalStep;
import no.systek.dataflow.steps.ListStep;
import no.systek.dataflow.steps.PairJoinStep;

@SuppressWarnings({ "WeakerAccess", "SameParameterValue", "unused" })
public final class Steps {

    public static <I, O> Step<I, O> newSingle(Function<I, O> func) {
        return new SingleStep<I, O>(null) {
            @Override
            O execute(I input) {
                return func.apply(input);
            }
        };
    }

    public static <I, O> Step<I, O> newParallel(Function<I, O> func) {
        return new ParallelStep<I, O>(null) {
            @Override
            O execute(I input) {
                return func.apply(input);
            }
        };
    }

    public static <T> CollectorStep<T> newCollector(int bufferSize) {
        return new CollectorStep<>(null, bufferSize);
    }

    public static <Ileft, Iright, O> PairJoinStep<Ileft, Iright, O> newJoiner(
        Predicate<Object> isLeft,
        BiFunction<Ileft, Iright, O> func) {

        return new PairJoinStep<Ileft, Iright, O>(null) {
            @Override
            protected O join(Ileft left, Iright right) {
                return func.apply(left, right);
            }

            @Override
            protected boolean isLeft(Object input) {
                return isLeft.test(input);
            }
        };
    }

    public static <T> SimpleConditionalStep<T> newCondition(Predicate<T> test) {
        return new SimpleConditionalStep<T>(null) {
            @Override
            boolean test(T input) {
                return test.test(input);
            }
        };
    }

    public static <I, O> ListStep<I, O> newParallelListStep(Function<List<I>, List<O>> func) {
        return new ListStep<I, O>(null) {
            @Override
            protected List<O> execute(List<I> in) {
                return func.apply(in);
            }
        };
    }

    public static abstract class SimpleConditionalStep<T> extends ConditionalStep<T, T> {

        public SimpleConditionalStep(String name) {
            super(name, Integer.MAX_VALUE);
        }

        @Override
        protected void run(T input, BiConsumer<Boolean, T> onResult) {
            onResult.accept(test(input), input);
        }

        abstract boolean test(T input);

    }

    public static abstract class ParallelStep<I, O> extends SimpleStep<I, O> {
        public ParallelStep(String name) {
            super(name, Integer.MAX_VALUE);
        }
    }

    public static abstract class SingleStep<I, O> extends SimpleStep<I, O> {
        public SingleStep(String name) {
            super(name, 1);
        }
    }

    public static abstract class SimpleStep<I, O> extends Step<I, O> {

        public SimpleStep(String name, int maxParallelExecution) {
            super(name, maxParallelExecution);
        }

        @Override
        protected void run(I input, Consumer<O> onResult) {
            onResult.accept(execute(input));
        }

        abstract O execute(I input);
    }

}
