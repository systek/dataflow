package no.systek.dataflow;

import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;

import org.junit.Test;

import no.systek.dataflow.steps.CollectorStep;
import no.systek.dataflow.steps.PairJoinStep;
import no.systek.dataflow.types.BlackCoffee;
import no.systek.dataflow.types.Cappuccino;
import no.systek.dataflow.types.FoamedMilk;
import no.systek.dataflow.types.GrindedCoffee;
import no.systek.dataflow.types.Order;
import no.systek.dataflow.types.Water;

public class CappuccinoTest extends AbstractStepTest {

    @Test
    public void makeOneCappuccino() {
        final Random random = new Random(203);

        /*
         * The following graph of steps produces one cappuccino:
         *
         *  TapWater           GrindBeans           FoamMilk
         *     |                    |                   |
         *     v                    |                   |
         * HeatWater <------+       |                   |
         *     |            |       |                   |
         *     v            |       |                   |
         * HotEnough? --no--+       |                   |
         *     |                    |                   |
         *    yes                   |                   |
         *     |                    v                   |
         *     +----------------> brew                  |
         *                          |                   |
         *                          v                   |
         *                      Cappuccino <------------+
         *
         * credit: http://stackoverflow.com/questions/10855045/executing-dependent-tasks-in-parallel-in-java
         *
         */

        TapWater tapWater = new TapWater();
        FoamMilk foamMilk = new FoamMilk();
        GrindBeans grindBeans = new GrindBeans();

        HotEnough hotEnough = new HotEnough();

        CoffeeBrewer brew = new CoffeeBrewer();

        Step<Water, Water> heatWater = Steps.newParallel(water ->
            work(new Water(random.nextInt(10) + 90), "Heating water"));

        CappuccinoStep cappuccino = new CappuccinoStep();

        // Setup execution plan by setting the tasks dependencies
        cappuccino.dependsOnLeft(brew.output());
        cappuccino.dependsOnRight(foamMilk.output());

        brew.dependsOnLeft(grindBeans.output());
        brew.dependsOnRight(hotEnough.ifTrue());

        hotEnough.dependsOn(heatWater.output());
        heatWater.dependsOn(hotEnough.ifFalse());
        heatWater.dependsOn(tapWater.output());

        assertThat(stepExecutor.execute(cappuccino, new Order("CappuccinoOrder")), notNullValue());
    }

    @Test
    public void makeMultipleCappuccino() {
        final Random random = new Random(203);

        /*
         * Let's make multiple cappuccinos in parallel. The water boiler can boil water for two cups at once, so it
         * waits until two inputs are available.
         *
         * OrderSplitter
         *   |
         *   |
         *   +--> GrindBeans-------------------------------------------+
         *   |                                                         |
         *   +--> TapWater                                             |
         *   |        |                                                |
         *   |        v                                                v
         *   |    Collect(2)----> HeatWater----> HotEnough?---yes---> Brew
         *   |        ^                               |                |
         *   |        +-------------------------no----+                |
         *   |                                                         v
         *   +--> FoamMilk--------------------------------------> Cappuccino
         *
         */

        TapWater tapWater = new TapWater();
        FoamMilk foamMilk = new FoamMilk();
        GrindBeans grindBeans = new GrindBeans();

        HotEnough hotEnough = new HotEnough();

        CoffeeBrewer brew = new CoffeeBrewer();
        Step<List<Water>, Water> heatWater = Steps.newParallelListStep(waters -> work(
            waters.stream().map(water -> new Water(random.nextInt(10) + 90)).collect(Collectors.toList()),
            "heating multiple waters at once (" + waters.size() + ")"));

        CollectorStep<Water> collector = Steps.newCollector(2);
        Step<List<Order>, Order> orderSplitter = Steps.newParallelListStep(orders -> orders);

        CappuccinoStep cappuccino = new CappuccinoStep();

        // Setup execution plan by setting the tasks dependencies
        cappuccino.dependsOnLeft(brew.output());
        cappuccino.dependsOnRight(foamMilk.output());

        brew.dependsOnLeft(grindBeans.output());
        brew.dependsOnRight(hotEnough.ifTrue());

        hotEnough.dependsOn(heatWater.output());

        heatWater.dependsOn(collector.output()); // water heater is shared, heats for 2 orders at the same time

        collector.dependsOn(hotEnough.ifFalse());
        collector.dependsOn(tapWater.output());

        tapWater.dependsOn(orderSplitter.output());
        grindBeans.dependsOn(orderSplitter.output());
        foamMilk.dependsOn(orderSplitter.output());

        List<Cappuccino> cappuccinos = stepExecutor.executeList(cappuccino, Arrays.asList(
            new Order("Order1"),
            new Order("order2"),
            new Order("order3")));
        assertThat(cappuccinos.size(), is(3));
    }

    private static <T> T work(T doneValue, String task) {
        log(task, "...");
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        log(task, "...done");
        return doneValue;
    }

    private static void log(String... msg) {
        System.out.println(Thread.currentThread().getName() + ": " + String.join(" ", msg));
    }


    /*
     * Defining small and reusable steps for the tests
     */

    private static class HotEnough extends Steps.SimpleConditionalStep<Water> {

        HotEnough() {
            super(null);
        }

        @Override
        boolean test(Water water) {
            boolean result = water.temperature > 95;
            log("Water hot enough? " + result);
            return result;
        }
    }

    private static class TapWater extends Steps.SimpleStep<Order, Water> {

        TapWater() {
            super(null, Integer.MAX_VALUE);
        }

        @Override
        Water execute(Order order) {
            return work(new Water(18), "tapping water for " + order);
        }
    }

    private static class CoffeeBrewer extends PairJoinStep<GrindedCoffee, Water, BlackCoffee> {
        CoffeeBrewer() {
            super(null);
        }

        @Override
        protected BlackCoffee join(GrindedCoffee grindedCoffee, Water water) {
            return work(new BlackCoffee(grindedCoffee, water), "brewing coffee");
        }

        @Override
        protected boolean isLeft(Object input) {
            return input instanceof GrindedCoffee;
        }
    }

    private static class CappuccinoStep extends PairJoinStep<BlackCoffee, FoamedMilk, Cappuccino> {
        CappuccinoStep() {
            super(null);
        }

        @Override
        protected Cappuccino join(BlackCoffee blackCoffee, FoamedMilk foamedMilk) {
            return work(new Cappuccino(blackCoffee, foamedMilk), "making cappuccino");
        }

        @Override
        protected boolean isLeft(Object input) {
            return input instanceof BlackCoffee;
        }
    }

    private final class GrindBeans extends Steps.SimpleStep<Order, GrindedCoffee> {
        GrindBeans() {
            super(null, Integer.MAX_VALUE);
        }

        @Override
        GrindedCoffee execute(Order order) {
            return work(new GrindedCoffee(order), "grinding coffee for " + order);
        }
    }

    private final class FoamMilk extends Steps.SimpleStep<Order, FoamedMilk> {
        FoamMilk() {
            super(null, Integer.MAX_VALUE);
        }

        @Override
        FoamedMilk execute(Order order) {
            return work(new FoamedMilk(order), "foaming milk for " + order);
        }
    }

}
