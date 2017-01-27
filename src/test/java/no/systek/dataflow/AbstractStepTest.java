package no.systek.dataflow;

import org.junit.After;
import org.junit.Before;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public abstract class AbstractStepTest {

    private ExecutorService executorService;
    protected StepExecutor stepExecutor;

    @Before
    public void setup() {
        executorService = Executors.newFixedThreadPool(5);
        stepExecutor = new StepExecutor(executorService, s -> {
        }, () -> null, 5, 20, TimeUnit.SECONDS);
    }

    @After
    public void cleanup() {
        executorService.shutdown();
        executorService = null;
        stepExecutor = null;
    }

}
