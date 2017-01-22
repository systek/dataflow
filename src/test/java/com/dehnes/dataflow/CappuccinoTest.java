package com.dehnes.dataflow;

import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Random;
import java.util.stream.Collectors;

import static org.hamcrest.core.Is.is;

public class CappuccinoTest extends AbstractStepTest {

    @Test
    public void makeOneCappuccino() {
        final Random random = new Random(203);

        /*
         * The following graph of steps produces one cappuccino:
         *
         * GrindBeans---------------------------+
         *                                      v
         * HeatWater----> HotEnough?---yes---> brew ------+
         *     ^              |                           |
         *     +---------no---+                           |
         *                                                v
         * FoamMilk--------------------------------> Cappuccino
         *
         *
         * credit: http://stackoverflow.com/questions/10855045/executing-dependent-tasks-in-parallel-in-java
         *
         */

        Step grindBeans = Steps.newParallel(order -> work("coffee", "grinding coffee for " + order));
        Step heatWater = Steps.newParallel(order -> work("heated water", "Heating water for " + order));
        Step foamMilk = Steps.newParallel(order -> work("foamed milk", "Foaming milk for " + order));
        Step brew = Steps.newJoiner(Arrays.asList(
                o -> o.equals("coffee"),
                o -> o.equals("heated water")
        ), l -> work("black-coffee", "brewing"));
        Step cappuccino = Steps.newJoiner(Arrays.asList(
                o -> o.equals("black-coffee"),
                o -> o.equals("foamed milk")
        ), l -> work("done-coffee", "combining"));
        ConditionalStep waterHotEnough = Steps.newCondition(i -> {
            boolean result = random.nextBoolean();
            log("water hot enough?: ", String.valueOf(result));
            return result;
        });

        // Setup execution plan by setting the tasks dependencies
        cappuccino.dependsOn(brew.output());
        cappuccino.dependsOn(foamMilk.output());

        brew.dependsOn(waterHotEnough.ifTrue());
        brew.dependsOn(grindBeans.output());

        waterHotEnough.dependsOn(heatWater.output());
        heatWater.dependsOn(waterHotEnough.ifFalse());

        Assert.assertThat(run(cappuccino, "CappuccinoOrder", 5).size(), is(1));
    }

    @Test
    public void makeMultipleCappuccino() {
        final Random random = new Random(203);

        /*
         * Let's make multiple cappuccinos in parallel. The water boiler can boil water for two cups at once, so it waits
         * until two inputs are available.
         *
         * OrderSplitter
         *   |
         *   |
         *   +--> GrindBeans-------------------------------------------+
         *   |                                                         v
         *   +--> Collect(2)----> HeatWater----> HotEnough?---yes---> Brew
         *   |         ^                              |                |
         *   |         +------------------------no----+                |
         *   |                                                         v
         *   +--> FoamMilk--------------------------------------> Cappuccino
         *
         */

        Step grindBeans = Steps.newParallel(order -> work("coffee", "grinding coffee for " + order));
        Step heatWater = Steps.newParallelListStep(
                orders -> work(orders.stream().map(o -> "heated water").collect(Collectors.toList()),
                        "heating water for " + orders.size() + " orders"));
        Step foamMilk = Steps.newParallel(order -> work("foamed milk", "Foaming milk for " + order));
        Step blackCoffee = Steps.newJoiner(Arrays.asList(
                o -> o.equals("coffee"),
                o -> o.equals("heated water")
        ), l -> work("black-coffee", "brewing"));
        Step cappuccino = Steps.newJoiner(Arrays.asList(
                o -> o.equals("black-coffee"),
                o -> o.equals("foamed milk")
        ), l -> work("done-coffee", "combining"));
        ConditionalStep waterHotEnough = Steps.newCondition(i -> {
            boolean result = random.nextBoolean();
            log("water hot enough?: ", String.valueOf(result));
            return result;
        });

        Step heatWaterOrderCollector = Steps.newCollector(2);
        Step orderSplitter = Steps.newParallelListStep(orders -> orders);

        // Setup execution plan by setting the tasks dependencies
        cappuccino.dependsOn(blackCoffee.output());
        cappuccino.dependsOn(foamMilk.output());

        blackCoffee.dependsOn(waterHotEnough.ifTrue());
        blackCoffee.dependsOn(grindBeans.output());

        waterHotEnough.dependsOn(heatWater.output());

        heatWater.dependsOn(heatWaterOrderCollector.output()); // water heater is shared, heats for 2 orders at the same time
        heatWaterOrderCollector.dependsOn(waterHotEnough.ifFalse());

        heatWaterOrderCollector.dependsOn(orderSplitter.output());
        grindBeans.dependsOn(orderSplitter.output());
        foamMilk.dependsOn(orderSplitter.output());

        Assert.assertThat(run(cappuccino, Arrays.asList("Order1", "order2", "order3"), 5).size(), is(3));
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

}
