package no.systek.dataflow;

import no.systek.dataflow.steps.PairJoinStep;
import no.systek.dataflow.steps.SourceStep;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.IntStream;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class StepTest extends AbstractStepTest {

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

    @Test
    public void sourceStepTest() {
        Assert.assertThat(stepExecutor.execute(Steps.newSource(() -> "Hello world")), is("Hello world"));
    }

    @Test
    public void singleStepTest() {
        Assert.assertThat(stepExecutor.execute(Steps.<Integer, Integer>newSingle(in -> in + 1), 1), is(2));
    }

    @Test
    public void joinerTest() {
        PairJoinStep<String, String, String> joinStep = Steps.newJoiner("Hello"::equals, (left, right) -> left + " " + right);

        joinStep.dependsOnLeft(Steps.newSource(() -> "Hello").output());
        joinStep.dependsOnRight(Steps.newSource(() -> "World").output());

        assertThat(stepExecutor.execute(joinStep), is("Hello World"));
    }

    @Test
    public void conditionTest() {
        Steps.SimpleConditionalStep<String> condition = Steps.newCondition("Hello"::equals);
        Step<String, Object> sink = new Step<String, Object>(1) {
            @Override
            protected void run(String input, Consumer<Object> onResult) {
            }
        };
        Step<String, String> tail = Steps.newSingle(input -> input);

        tail.dependsOn(condition.ifTrue());
        sink.dependsOn(condition.ifFalse());
        condition.dependsOn(Steps.newSource(() -> "Hello").output());
        condition.dependsOn(Steps.newSource(() -> "World").output());

        assertThat(stepExecutor.execute(tail), is("Hello"));
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
