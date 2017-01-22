package com.dehnes.dataflow;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public abstract class AbstractStepTest {

    List<Object> run(Step tail, Object input, int treads) {
        ExecutorService executorService = Executors.newFixedThreadPool(treads);
        try {
            List<Object> results = new LinkedList<>();
            assertThat(tail.executeTasksAndAwaitDone(
                    new PriorityTaskQueue(treads, () -> null, s -> {
                    }),
                    executorService,
                    Throwable::printStackTrace,
                    input,
                    results::add,
                    10,
                    TimeUnit.SECONDS
            ), is(true));
            return results;
        } finally {
            executorService.shutdown();
        }

    }
}
