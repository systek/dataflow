package no.systek.dataflow;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * Helper to execute a graph of steps with default error handling
 */
public class StepExecutor {
    private final static Logger LOGGER = LoggerFactory.getLogger(StepExecutor.class);

    private final ExecutorService executorService;
    private final Consumer<String> correlationIdSettter;
    private final Supplier<String> correlationIdGetter;
    private final int maxParallelTasks;
    private final long timeout;
    private final TimeUnit timeUnit;

    public StepExecutor(
            ExecutorService executorService,
            Consumer<String> correlationIdSettter,
            Supplier<String> correlationIdGetter,
            int maxParallelTasks,
            long timeout,
            TimeUnit timeUnit) {

        this.executorService = executorService;
        this.correlationIdSettter = correlationIdSettter;
        this.correlationIdGetter = correlationIdGetter;
        this.maxParallelTasks = maxParallelTasks;
        this.timeout = timeout;
        this.timeUnit = timeUnit;
    }

    public List<Object> executeList(Step tail, Object input) {

        List<Exception> exceptions = new CopyOnWriteArrayList<>();
        List<Object> results = new CopyOnWriteArrayList<>();

        if (!tail.executeTasksAndAwaitDone(
                new PriorityTaskQueue(maxParallelTasks, correlationIdGetter, correlationIdSettter),
                executorService,
                exceptions::add,
                input,
                results::add,
                timeout,
                timeUnit)) {
            throw new RuntimeException("Timeout during execution");
        }

        if (!exceptions.isEmpty()) {
            exceptions.forEach(e -> LOGGER.error("", e));
            throw new RuntimeException("One or more exceptions caught during execution, see logging");
        }

        return new LinkedList<>(results);
    }

    public List<Object> executeList(Step tail) {
        return executeList(tail, new Object());
    }

    public Object execute(Step tail, Object input) {
        List<Object> results = executeList(tail, input);
        return results.isEmpty() ? null : results.get(0);
    }

    public Object execute(Step tail) {
        return execute(tail, new Object());
    }

}
