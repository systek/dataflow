package com.dehnes.dataflow;

import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

public final class Steps {

    public static Step newSingle(Function<Object, Object> func) {
        return new SingleStep(null) {
            @Override
            Object execute(Object input) {
                return func.apply(input);
            }
        };
    }

    public static Step newParallel(Function<Object, Object> func) {
        return new ParallelStep(null) {
            @Override
            Object execute(Object input) {
                return func.apply(input);
            }
        };
    }

    public static Step newCollector(int bufferSize) {
        return new CollectorStep(null, bufferSize);
    }

    public static Step newJoiner(List<Predicate<Object>> filters, Function<List<Object>, Object> func) {
        return new JoinStep(null, filters) {
            @Override
            Object join(List<Object> list) {
                return func.apply(list);
            }
        };
    }

    public static ConditionalStep newCondition(Predicate<Object> test) {
        return new SimpleConditionalStep(null) {
            @Override
            Boolean test(Object input) {
                return test.test(input);
            }
        };
    }

    public static Step newParallelListStep(Function<List<Object>, List<Object>> func) {
        return new ListStep(null) {
            @Override
            List<Object> execute(List<Object> in) {
                return func.apply(in);
            }
        };
    }

    public static abstract class ListStep extends Step {

        public ListStep(String name) {
            super(name, Integer.MAX_VALUE);
        }

        @Override
        protected void run(Object input, Consumer<Object> onResult) {
            //noinspection unchecked
            execute((List<Object>) input).forEach(onResult);
        }

        abstract List<Object> execute(List<Object> in);
    }

    public static abstract class SimpleConditionalStep extends ConditionalStep {

        public SimpleConditionalStep(String name) {
            super(name, Integer.MAX_VALUE);
        }

        @Override
        protected void run(Object input, BiConsumer<Boolean, Object> onResult) {
            onResult.accept(test(input), input);
        }

        abstract Boolean test(Object input);

    }

    public static abstract class JoinStep extends Step {

        private final List<Predicate<Object>> filters;
        private final List<Queue<Object>> buckets;

        public JoinStep(String name, List<Predicate<Object>> filters) {
            super(name, 1);
            this.filters = filters;
            this.buckets = new LinkedList<>();
            filters.forEach(f -> buckets.add(new LinkedList<>()));
        }

        abstract Object join(List<Object> list);

        @Override
        protected void run(Object input, Consumer<Object> onResult) {
            boolean foundBucket = false;
            for (int i = 0; i < filters.size(); i++) {
                Predicate<Object> p = filters.get(i);
                if (p.test(input)) {
                    foundBucket = true;
                    buckets.get(i).offer(input);
                    break;
                }
            }
            if (!foundBucket) {
                throw new RuntimeException("Input " + input + " did not fall into any of the buckets!");
            }

            while (true) {

                boolean foundEmptyBucket = false;
                for (Queue<Object> bucket : buckets) {
                    if (bucket.peek() == null) {
                        foundEmptyBucket = true;
                        break;
                    }
                }

                if (foundEmptyBucket) {
                    break;
                }

                List<Object> row = new LinkedList<>();
                buckets.forEach(b -> row.add(b.poll()));
                onResult.accept(join(row));
            }

        }

    }

    public static class CollectorStep extends Step {
        private static final String CLEANUP = "CLEANUP";

        private final List<Object> items;
        private final int bufferSize;
        private boolean scheduledCleanup;

        public CollectorStep(String name, int bufferSize) {
            super(name, 1);
            this.bufferSize = bufferSize;
            this.items = new LinkedList<>();
        }

        @Override
        protected void run(Object input, Consumer<Object> onResult) {
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
        protected void afterRun(PriorityTaskQueue priorityTaskQueue, Consumer<Object> onFinalResult) {
            // schedule a cleanup tasks at a lower priority such that if there are no more
            // steps executing (thus waiting for some input), then this step should run using
            // the items it got so far.

            // do this only once, the the first item is scheduled
            if (items.size() == 1 && !scheduledCleanup) {
                scheduledCleanup = true;
                priorityTaskQueue.addTask(getGraphDepth(), pq2 -> post(CLEANUP, pq2, onFinalResult));
            }
        }

        private void pushItems(Consumer<Object> onResult) {
            List<Object> items = new LinkedList<>(this.items);
            this.items.clear();
            onResult.accept(items);
        }
    }

    public static abstract class ParallelStep extends SimpleStep {

        public ParallelStep(String name) {
            super(name, Integer.MAX_VALUE);
        }

    }

    public static abstract class SingleStep extends SimpleStep {

        public SingleStep(String name) {
            super(name, 1);
        }
    }

    public static abstract class SimpleStep extends Step {

        public SimpleStep(String name, int maxParallelExecution) {
            super(name, maxParallelExecution);
        }

        @Override
        protected void run(Object input, Consumer<Object> onResult) {
            onResult.accept(execute(input));
        }

        abstract Object execute(Object input);
    }

}
