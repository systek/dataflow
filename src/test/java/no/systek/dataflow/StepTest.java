package no.systek.dataflow;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import org.junit.Test;

import no.systek.dataflow.steps.SourceStep;

public class StepTest {

    @Test
    public void treeIsConfiguredCorrectly() {
        List<Step<Object, Object>> steps = new LinkedList<>();

        IntStream.range(1, 10).forEach(level -> {
            steps.add(createStep(level));
            if (steps.size() > 1) {
                steps.get(steps.size() - 1).dependsOn(steps.get(steps.size() - 2).output());
            }
        });

        HashSet<Step<Object, ?>> roots = new HashSet<>();
        steps.get(steps.size() - 1).configureTreeAndFindRoots(new HashSet<>(), roots);

        assertThat(roots.size(), is(1));
        AtomicInteger assertLevel = new AtomicInteger(1);
        steps.forEach(s -> {
            int expectedDepth = assertLevel.getAndIncrement();
            assertThat(s.getName(), is(String.valueOf(expectedDepth)));
            assertThat(s.getGraphDepth(), is(expectedDepth));
        });
    }

    private Step<Object, Object> createStep(int level) {
        return new SourceStep<Object>(String.valueOf(level), Integer.MAX_VALUE) {
            @Override
            protected Object get() {
                return null;
            }
        };
    }

}
