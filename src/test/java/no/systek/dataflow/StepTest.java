package no.systek.dataflow;

import no.systek.dataflow.Step;
import no.systek.dataflow.Steps;
import org.junit.Test;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class StepTest {

    @Test
    public void treeIsConfiguredCorrectly() {
        List<Step> steps = new LinkedList<>();

        IntStream.range(1, 10).forEach(level -> {
            steps.add(createStep(level));
            if (steps.size() > 1) {
                steps.get(steps.size() - 1).dependsOn(steps.get(steps.size() - 2).output());
            }
        });

        HashSet<Step> roots = new HashSet<>();
        steps.get(steps.size() - 1).configureTreeAndFindRoots(new HashSet<>(), roots);

        assertThat(roots.size(), is(1));
        AtomicInteger assertLevel = new AtomicInteger(1);
        steps.forEach(s -> {
            int expectedDepth = assertLevel.getAndIncrement();
            assertThat(s.getName(), is(String.valueOf(expectedDepth)));
            assertThat(s.getGraphDepth(), is(expectedDepth));
        });
    }

    private Step createStep(int level) {
        return new Steps.SimpleStep(String.valueOf(level), 1) {
            @Override
            Object execute(Object input) {
                return null;
            }
        };
    }

}
