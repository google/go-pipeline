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
The entire pipeline is invoked with an executor such as `Do()`.

Producers and stages may return errors.  An erroring producer or stage ceases
to produce work items for downstream use, and discards all work items it
receives; in this way, as soon as an error occurs anywhere in the pipeline,
the rest of the pipeline drains and the executor returns the first captured
error.

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

## Tell me more

A demonstration of how the pipeline works is included at ./demo/demo.go.  This
program builds a prefix tree from a provided dictionary of (utf8) words.  This
task is difficult to parallelize, since adding each word requires updating the
entire path from the prefix tree root down to the word's node.  We could use
a lock at each node, but this would only hurt: not only would it significantly
harm the space and time requirements of working with the tree, but lock
contention would increase in the higher levels of the tree, to the point that
the root node's lock would be overwhelmed (every word starts with '').

Instead, we can build a pipeline.  In this pipeline, the *work item* will be
a batch of words from the dictionary, bundled with a local prefix tree.  We'll
use three pipeline stages:

*   A producer that divides up the word list into batches, then prepares the
    work items and sends them downstream;
*   A 'local insert' stage that fills the prefix tree within the work item from
    the work item's word batch;
*   A 'global merge' stage that merges the work item's prefix tree into the
    global prefix tree.
    
These local prefix trees can be large and complex, and creating a new one for
each batch might introduce too much allocation and GC overhead.  So, this
application is a good candidate for a recycling producer; this means we also
need to provide a way to clear out a work item's local prefix tree.  Here's how
this recycling producer function might look:

```go
producerFn := func(get func() (*batch, bool), put func(*batch)) error {
    for i := 0; i < len(words); i += *batchSize {
        b, ok := get()
        if ok {
   			b.root.Clear()
   		} else {
   			b = &batch{
   				root: prefixtree.New(),
   			}
   		}
   		end := i + *batchSize
   		if end > len(words) {
   			end = len(words)
   		}
   		b.words = words[i:end]
   		put(b)
   	}
   	return nil
}
```

The stage functions are straightforward::

```go
localInsertFn := func(in *batch) (out *batch, err error) {
   	for _, word := range in.words {
   	   	in.root.Insert(word)
   	}
   	return in, nil
}
globalMergeFn := func(in *batch) (out *batch, err error) {
   	root.MergeFrom(in.root)
   	return in, nil
}
```

We can then turn these functions into pipeline stages, and attach options:

```go
producer := pipeline.NewRecyclingProducer(
    producerFn,
    pipeline.Name("batch production"),
	pipeline.InputBufferSize(1),
)
localInsertStage := pipeline.NewStage(
	localInsertFn,
    pipeline.Name("local insert"),
	pipeline.Concurrency(2),
	pipeline.InputBufferSize(1),
)
globalMergeStage := pipeline.NewStage(
	globalMergeFn,
    pipeline.Name("global merge"),
	pipeline.InputBufferSize(1),
)
```

And then invoke the pipeline using an *executor function*, `pipeline.Do()` or
`pipeline.Measure()`:

```go
if err := pipeline.Do(
    producer,
	localInsertStage,
	globalMergeStage,
); err != nil {
    log.Fatalf("Do() failed: %s", err)
}
```

Note that declaring the producer and stage functions, defining the producer and
stages, and invoking the executor could be all done in one go; they're
separated them here for reusability and clarity.

`demo.go` provides three invocation modes: `serial`, which does no
parallelization at all; `concurrenct`, which uses `pipeline.Do()`, and
`measure`, which uses `pipeline.Measure()`.  The latter prints out pipeline
statistics after execution, e.g.

```
go run . --dict_filename words.txt --mode measure
2023/02/08 12:20:18 Pipeline wall time: 768.044933ms
  batch production (0): 94 items, total 768.01492ms (8.170371ms/item), work 209.897747ms (2.232954ms/item)
  local insert (0)    : 47 items, total 760.165039ms (16.173724ms/item), work 468.84056ms (9.975331ms/item)
  local insert (1)    : 47 items, total 753.511622ms (16.032162ms/item), work 461.03055ms (9.80916ms/item)
  global merge (0)    : 94 items, total 768.014162ms (8.170363ms/item), work 750.937744ms (7.988699ms/item)
2023/02/08 12:20:18 Prefix tree construction took 768.194396ms.  Added 466551 words.
```

The main value to look for here is the largest `work` duration in any single
pipeline; this (together with available concurrency e.g. via `GOMAXPROCS`)
is the main factor limiting overall pipeline execution time: the tree took
769ms to construct, of which 751ms was due to the `global merge` stage.  Note
that there are two instances of the `local insert` stage; this is because we
specified
`pipeline.Concurrency(2)` for that stage.

To reiterate a point from earlier: pipeline parallelization is not trivial!
The `concurrent` mode of `demo` will only outperform the `serial` mode for very
large input dictionaries, and to get `concurrent` to significantly outperform
`serial` we'd need to figure out some way to split the `global merge` stage:
perhaps by partitioning the global tree's nodes into groups, with one pipeline
stage configured to update each group.
