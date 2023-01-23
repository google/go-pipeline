# Pipeline

Tools for building, executing, and tuning single-node concurrent software
pipelines.

## What is this?

Software pipelining is a programming technique in which a sequence, or 
*pipeline*, of computations is set up such that each stage works on input from
its predecessor, then passes its results on to its successor.  It is often
compared to assembly lines, with which it has many common features.

Software pipelines can be used for parallelization.  Since all pipeline stages
execute concurrently with one another, a well-tuned software pipeline can ensure
that shared resources have high utilization, and that useful work can be done
on non-shared data between accesses to shared data.

This package provides tools for implementing software-pipeline-based
parallelizations.

## Do I need it?

Pipeline parallelization is neither the easiest parallelization technique, nor
the most effective.  Many problems are better served by simpler and more
scalable techniques like loop parallelization.  But, of course, some problems
just don't decompose that easily.

This package may be useful when concurrency is required but no simple data
decomposition is apparent.  For example, when processing a large number of
inputs, each of which will affect a significant proportion of a large shared
structure, many conventional techniques might introduce so much synchronization
overhead that any benefit from additional concurrency is lost.  

## How does it work?

The central concept of this package is the *work item*, which moves through the
pipeline receiving work from each stage in turn, like a car moving through an
assembly line.  In this package, all stages produce and consume the same work
item type, which is a type parameter constrained to `any`.  (In general,
software pipelines do not require stages to have the same input and output type,
but note that the work item type may union several different types to
approximate a more heterogeneous pipeline.)

The pipeline starts with a single *producer*, which generates new work items and
decides when no more work items should enter the pipeline.  The rest of the
pipeline is a series of *stages*, each of which draws from its predecessor,
with the initial stage drawing from the producer.  Each produced work item will
visit each stage in turn, before being retired out of the tail of the pipeline.

Work items may be large and complex, and expensive to allocate and set up.  To
avoid this expense, producers may *recycle* work items that have been retired
from the pipeline.  If a recycling producer is used, retiring work items will
be provided to the producer to reuse.

A pipeline's efficiency depends on its stages (including its producer) being
well-balanced.  The number of producer and stage workers in a pipeline set the
upper bound of its concurrency, but if some stages take longer than others to
complete, the achievable concurrency will decrease.  For example, if a simple
pipeline has five stages, of which the first four, A-D, take 1 second per work
item and the last, E, takes 5 seconds per work item, then during steady-state
operation, stages A-D will sit idle for 80% of the time, leading to an actual
concurrency of only 1.8.

In addition to algorithmic techniques like batching, this package provides three
tools for tuning its pipelines:

*   `Measure()`: This package provides automatic instrumentation and reporting
    to reveal the time each stage (including producer) spent working.  This
    helps tuners quickly identify which stages should be combined, split, or
    rethought. 
*   `InputBufferSize()`: The input buffer size of each stage (and recycling
    producers) is configurable.  Adding extra buffer between stages can help
    absorb variance in stage duration.
*   `Concurrency()`: Some stages, particularly those that do not work on shared
    state, can be replicated.  In the example above, if the work done by stage E
    can safely be done on multiple work items concurrently, then setting E's
    concurrency to 5 would increase utilization of stages A-D to 100% and yield
    an effective concurrency of 9.
