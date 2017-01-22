# dataflow

A small library, which lets you define tasks (called steps) and their dependencies among each other, 
and then execute them (in parallel if allowed) according to their dependency graph.

## "Step"
Similar to "[actors](https://en.wikipedia.org/wiki/Actor_model)", a step is a piece of work which is executed as some 
input arrives and can produce one or more outputs during execution.

Steps can be chained together to (complex) graphs by defining dependencies between then, including loop scenarios which 
are really useful for optimistic locking. 

## Simple Example

From the [CappuccinoTest](https://github.com/systek/dataflow/blob/master/src/test/java/com/dehnes/dataflow/CappuccinoTest.java#L18), 
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
It is also possible to use conditions and loops. If the heated water is not hot enough, it is re-heated again.

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
Like in the actor-model, a step has a mailbox in which inbound input values are queued. As soon as a new input value is queued
in this mailbox, the step gets ready to be executed.

All steps which are ready to be executed, thus not awaiting some input, are executed in parallel. The max number of concurrent executions is configurable via 
the included task scheduler [PriorityTaskQueue](https://github.com/systek/dataflow/blob/master/src/main/java/com/dehnes/dataflow/PriorityTaskQueue.java)). 

Unlike actors, where a single actor can never be executed in parallel, a step ***can*** be executed in parallel as soon as more
input values become available while it is already being executed. This is configurable by setting the step property "maxParallelExecution" to
larger than 1. Of course any internal state in the step becomes now subject to concurrent access and must be protected accordingly.


