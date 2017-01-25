# dataflow

A small library, which lets you define tasks (called steps) and their dependencies among each other, 
and then execute them (in parallel if allowed) according to their dependency graph.

## Usage
```
<dependency>
  <artifactId>dataflow</artifactId>
  <groupId>no.systek.dataflow</groupId>
  <version>0.1</version>
</dependency>
```

## "Step"
Similar to "[actors](https://en.wikipedia.org/wiki/Actor_model)", a step is a piece of work which is executed as some 
input arrives and can produce one or more outputs during execution.

Steps can be chained together to (complex) graphs by defining dependencies between then, including loop scenarios which 
are really useful for optimistic locking. 

## Simple Example

From the [CappuccinoTest](https://github.com/systek/dataflow/blob/master/src/test/java/no/systek/dataflow/CappuccinoTest.java#L18), 
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

## Conditions and loops
It is also possible to use conditions and loops to build more complex graphs. For example if the heated water is not hot enough, it is re-heated again.

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
Like in the actor-model, a step has a mailbox in which inbound input values are queued. As soon 
as a new input value is queued in this mailbox, the step gets ready to be executed.

All steps which are ready to be executed, thus not awaiting some input, are executed in parallel. 
The max number of concurrent executions is configurable via the included task scheduler 
[PriorityTaskQueue](https://github.com/systek/dataflow/blob/master/src/main/java/no/systek/dataflow/PriorityTaskQueue.java). 

Unlike actors, where a single actor can never be executed in parallel, a single step ***can*** be 
executed in parallel as soon as more input values become available while it is already being 
executed. This is configurable by setting the step property "maxParallelExecution" to larger 
than 1. Of course any internal state in the step becomes now subject to concurrent access and must 
be protected accordingly.

## Collectors
After a fork-out where processing is done in parallel, it might be desirable to join the output of those parallel steps again. This can be done with so called collector step. 

Image you want to process an order which contains multiple order lines. Each order line is processed in parallel but shipping and invoicing is only once done once for entire order:

```
orderLineSplitter -----> parallelOrderLineProcessor
                                   |
                                   v
                           lineNeedsShipping
                               |     |
               +------yes------+     no
               v                     |
       collectForShipping      collectNoShipping
               |                     |
               |                     v
          shipAllItems--------> createInvoice
                                     |
                                     v
                                 sendInvoice
                                     |
                                     v
                                  finished
```

```java
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

In this example, the step orderLineProcessor is permitted to execute in parallel. The orderLineSplitter takes the 
entire order as input and procudes outputs for each order line. As soon as a new order line is sendt to orderLineProcessor, processing starts in parallel. After order line processing, the results are collected depending on whether shipping is needed or not.

But how does a collector step know when to proceed, e.g. that there will be no more inputs arriving? 
This information is derived from the fact that there are no more steps executing and thus all
are awaiting more input. A collector step schedules a cleanup task in the taskScheduler with a lower pririty, 
which gets only executed once all other steps have finished. See [CollectorStep](https://github.com/systek/dataflow/blob/master/src/main/java/no/systek/dataflow/Steps.java#L172).

