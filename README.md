[![Build Status](https://travis-ci.org/systek/dataflow.svg?branch=master)](https://travis-ci.org/systek/dataflow)

# dataflow
A Java library, which lets you define dependencies between tasks (called steps) and then execute the entire graph. 
Steps which have no unresolved dependencies are executed first - in parallel.

## Usage
```
<dependency>
  <artifactId>dataflow</artifactId>
  <groupId>no.systek.dataflow</groupId>
  <version>0.1</version>
</dependency>
```

## Requirements
- Java8

There are no dependencies to other libraries except for SLF4j.

## "Step"
Similar to "[actors](https://en.wikipedia.org/wiki/Actor_model)", a step is a piece of work (/task) which is executed as 
some input arrives and can produce one or more outputs during execution.

Steps are then linked together to form a graph by defining dependencies between them. Each time a step produces an output, 
this output is then automatically distributed to it's child steps which get ready to be executed.

### Simple Example
From the [CappuccinoTest](https://github.com/systek/dataflow/blob/master/src/test/java/no/systek/dataflow/CappuccinoTest.java#L29), 
the following steps produce one cappuccino:

```
GrindBeans-------------+
                       v 
HeatWater-----------> brew ----+
                               |
                               v
FoamMilk-----------------> Cappuccino
```
This graph is defined in code like this:

```java
cappuccino.dependsOn(brew.output());
cappuccino.dependsOn(foamMilk.output());

brew.dependsOn(heatWater.output());
brew.dependsOn(grindBeans.output());
```
To fullfill the cappuccino step it needs input from both the brew and the foamMilk step. Only after both inputs 
are available, execution of the cappuccino step is scheduled.
And the brew step cannot start before it has received its required inputs from heatWater and GrindBeans. And so on.

## More complex graphs
A [Step](https://github.com/systek/dataflow/blob/master/src/main/java/no/systek/dataflow/Step.java) can be easily 
extended to achieve rich capabilities like *collectors*, *conditional routing* and even *loops*!.

### Conditional step
The library includes a [ConditionalStep](https://github.com/systek/dataflow/blob/master/src/main/java/no/systek/dataflow/steps/ConditionalStep.java) which allows for 2-way conditional routing. For example:

```
   PickAppleFromTree
          |
          v
    InGoodCondition?
          |
   +-yes--+--yes---+
   |               |
   v               v
ThrowAway       Collect
```

### Collector step
After a fork-out where processing is done in parallel, it might be desirable to join the output of those parallel steps again. This can be done with so called [CollectorStep](https://github.com/systek/dataflow/blob/master/src/main/java/no/systek/dataflow/steps/CollectorStep.java). 

For example, image you want to process an order which contains multiple order lines. Each order line is processed in parallel but shipping and invoicing is only once done once for entire order:

```
               orderLineSplitter
                      |
                      v
          parallelOrderLineProcessor
                      |
                      v
              lineNeedsShipping
                   |     |
         +--yes----+     +---no-----+
         |                          |
         v                          |
 collectForShipping                 v          
         |                   collectNoShipping
         v                          |
    shipAllItems                    |
         |                          |
         +------> createInvoice <---+
                       |
                       v
                  sendInvoice
                       |
                       v
                    finished
```

```java
// define the steps
Step collectForShipping = Steps.newCollector(Integer.MAX_VALUE)
Step collectNoShipping = Steps.newCollector(Integer.MAX_VALUE)
// ...

# setup their dependencies
finished.dependsOn(sendInvoice.output())

sendInvoice.dependsOn(createInvoice.output())

createInvoice.dependsOn(shipAllItems.output())
createInvoice.dependsOn(collectNoShipping.output())

collectNoShipping.dependsOn(lineNeedsShipping.ifFalse())

shipAllItems.dependsOn(collectForShipping.output())
collectForShipping.dependsOn(lineNeedsShipping.ifTrue())

lineNeedsShipping.dependsOn(parallelOrderLineProcessor.output())

parallelOrderLineProcessor.dependsOn(orderLineSplitter.output())

// and now execute the entire graph with order as input
finished.executeTasksAndAwaitDone(order);
```

In this example, the step parallelOrderLineProcessor is permitted to execute in parallel. The orderLineSplitter 
takes the entire order as input and procudes outputs for each order line. As soon as a new order line is 
sendt to parallelOrderLineProcessor, processing starts in parallel. After order line processing, the results are 
collected depending on whether shipping is needed or not.

#### Finish collecting?
But how does a collector step know when to proceed, e.g. that there will be no more inputs arriving? 
This information is derived from the fact that there are no more steps executing and thus all
are awaiting more input. A collector step schedules a cleanup task in the taskScheduler with a lower pririty, 
which gets only executed once all other steps have finished. See [CollectorStep](https://github.com/systek/dataflow/blob/master/src/main/java/no/systek/dataflow/steps/CollectorStep.java#L38).


### Loops
It is also possible to use conditions and loops to build more complex graphs. This is really useful when you have 
a more complex business transaction which require *optimistic locking*.

For example if the heated water is not hot enough, it is re-heated again.

```
GrindBeans---------------------------+
                                     v 
HeatWater----> HotEnough?---yes---> brew ------+
    ^              |                           |
    +---------no---+                           |
                                               v
FoamMilk--------------------------------> Cappuccino
```

Expressed in code like this:

```java
cappuccino.dependsOn(brew.output());
cappuccino.dependsOn(foamMilk.output());

brew.dependsOn(waterHotEnough.ifTrue());
brew.dependsOn(grindBeans.output());

waterHotEnough.dependsOn(heatWater.output());
heatWater.dependsOn(waterHotEnough.ifFalse());
```

## Parallel execution
Like in the actor-model, a step has a *mailbox* in which inbound input values are queued. As soon 
as a new input value is queued in this mailbox, the step gets ready to be executed.

All steps which are ready to be executed, thus not awaiting some input, are executed in parallel. 
The max number of concurrent executions is configurable via the included task scheduler 
[PriorityTaskQueue](https://github.com/systek/dataflow/blob/master/src/main/java/no/systek/dataflow/PriorityTaskQueue.java).
In our cappucino example above, GrindBeans, HeatWater and FoamMilk will start executing in parallel
 because those do not depend on each other.

Unlike actors, where a single actor can never be executed in parallel, a "step" in this library ***can*** be 
executed in parallel as soon as more input values become available while it is already being 
executed. This is configurable by setting the step property "maxParallelExecution" to larger 
than 1. Of course any internal state in the step becomes now subject to concurrent access and must 
be protected accordingly.

This is really usefull if you have a stateless step which is expected to process many inputs.
