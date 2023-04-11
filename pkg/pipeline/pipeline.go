/*
	Copyright 2023 Google Inc.

	Licensed under the Apache License, Version 2.0 (the "License");
	you may not use this file except in compliance with the License.
	You may obtain a copy of the License at

		https://www.apache.org/licenses/LICENSE-2.0

	Unless required by applicable law or agreed to in writing, software
	distributed under the License is distributed on an "AS IS" BASIS,
	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
	See the License for the specific language governing permissions and
	limitations under the License.
*/

// Package pipeline provides tools for rapidly assembling and tuning parallel
// software pipelines.
//
// # Software pipelines and work decomposition
//
// Many parallelization problems decompose via data parallelism: some
// data set is partitioned into independent chunks, each of which is processed
// by its own concurrent worker, and all such workers behave the same.
//
// Some parallelization problems, however, are not easily data-decomponsed.
// Most commonly, this is because no independent partition is available in the
// data set, or enabling such a partition is prohibitively complex.
//
// For some problems like this, software pipelines
// (https://en.wikipedia.org/wiki/Pipeline_(computing)) provide an alternative
// decomposition.  Rather than decomposing the data set into pieces handled by
// multiple independent and identical tasks, these instead decompose the work
// into multiple heterogeneous tasks which operate in sequence on a given work
// item; work items thus move through the pipeline from production to
// completion.  In this way, work that requires exclusive access can be done in
// parallel with work that doesn't.  Such systems are frequently compared to
// assembly lines: a single item moves through the pipeline, enjoying exclusive
// access to different resources at its different stops along the way, and over
// the duration of the pipeline the item is completed.
//
// Parallelism via data decomposition should be used wherever possible.  Work
// decomposition is considerably more nuanced and fragile, and the maximum
// concurrency achievable by work decomposition is limited by the nature of the
// decomposed work: typically this permits far less concurrency than data
// decomposition would.
//
// When data decomposition is not an option, however, this package can
// facilitate the construction and analysis of work-decomposing parallel
// pipelines.  The central concept of the pipeline is the work item, an object
// that moves through the pipeline and is worked on or mutated in turn by each
// stage.  In this package, the work item is type-parameterized in most types
// and functions, where it is constrained to `any`.
//
// # Building a software pipeline
//
// A pipeline comprises a single producer stage, followed by one or more
// intermediate stages.   The producer originates work items, then sends them
// into the remainder of the pipeline.  Each stage takes work items, does some
// work upon or with them, then sends them to the next stage.
//
// Producers must be able to create new work items, but may also support
// reusing work items that have already been through the pipeline; this is
// useful when work items' types are large or complex and allocation might
// dominate.   At minimum, a producer is a function (`ProducerFn`) accepting a
// `put func(T)` argument; invoking `put(i)` within the producer inserts `i`
// into the pipeline.  Recycling producers (`RecyclingProducerFn`) also accept
// a `get func() (i T, ok bool)` argument, a non-blocking function which
// returns a work item `i` that has retired from the end of the pipeline, or if
// no such retired work item is available, returns `false`.
//
// Subsequent stages in the pipeline implement `StageFn`, which accepts an
// input work item, performs work on that item, then returns it (or another
// instance of the same type reflecting the work done).
//
// # Running a software pipeline
//
// A pipeline is executed with `Do()`.  This function accepts a single
// producer, created with `NewProducer()` or `NewRecyclingProducer()`, and at
// least one stage, created with `NewStage()`.  These constructors accept a
// function of the appropriate type (`ProducerFn`, `RecyclingProducerFn`, and
// `StageFn`, respectively) as well as a variadic set of stage options such as
// the name of the stage.  A pipeline is linear, not branchy like true stream
// processing systems: each stage has at most one input and one output, and
// from the perspective of a single work item, the stages are processed in
// the order they appear in the arguments to `Do()`.
//
// Besides `Do()`, two other executor functions are defined in this package.
// `Measure()` works like `Do()`, but returns metrics about work time spent in
// each stage, and is useful for tuning a pipeline.  However, `Measure()`
// introduces profiling overhead and should be replaced with `Do()` once the
// tuning is complete.  Finally, `SequentialDo()` works like `Do()`, but with
// sequential, single-threaded execution and no synchronization overhead.  Like
// `Measure()`, `SequentialDo()` should only be used for assessment and tuning.
//
// # Designing and tuning a software pipeline
//
// As always when parallelizing, avoid too-fine-grained decompositions.  Each
// unit of work should be substantial enough to overcome the goroutine and
// channel overhead needed to parallelize it.
//
// The wall time of a software pipeline's execution will be proportional not to
// the total amount of work to be done divided by number of stages in the
// pipeline, but to the duration of the longest stage in the pipeline.  Suppose
// a task T, taking 7s, is decomposed into work items A, B, and C, with A
// taking 1s, B taking 2s, and C taking 4s.  Then the shortest overall pipeline
// duration we can hope for is the duration of C: 4s, significantly longer than
// the 7s/3=2.33s we might expect.  Indeed, in this case, there's no benefit to
// separating A and B as separate stages; even done together they do not
// dominate the pipeline.
//
// So, balancing work is the most important part of pipeline tuning, and the
// report generated by `Measure()` can help with this.  This package provides
// two other tools that can help:
//   - Buffering: Producers and stages can accept an `InputBufferSize` option.
//     By default, the buffers between stages can hold one work item; if a
//     stage attempts to write a second, it is blocked until the item already
//     in the buffer is removed.  By increasing the buffer size, the individual
//     stages are free to work at their own pace, which keeps the pipeline from
//     having to operate in lockstep and can absorb per-work-item variations.
//     Increasing the input buffer size can help when the overall pipeline
//     execution duration is significantly larger than its longest stage's
//     duration, but overall concurrency is still below GOMAXPROCS.
//   - Concurrency: Producers and stages can accept a `Concurrency` option.
//     By default, only one instance of each stage is used.  However, if the
//     work done in a given stage or producer can be done concurrently, this
//     package can set up multiple instances of that stage.  The number of
//     instances to use is specified with the `Concurrency` option.  So, if in
//     the example above it is safe to perform stage C concurrently on two
//     different work items, C can be run with `Concurrency(2)`.  This would
//     increase the available concurrency in the pipeline to 4, such that if
//     4 threads are free, the expected overall pipeline duration would fall to
//     2s (the maximum of [1s, 2s, 4s/2, 4s/2]).
//
// Examining the report from `Measure()` is the best way to understand how a
// pipeline is imbalanced and what might improve it.
package pipeline

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"
)

// ProducerFn performs the work of the initial stage in a pipeline, preparing
// work items of type T for processing.  Work items are not reused; if work
// items are expensive to produce, RecyclingProducerFn should be used instead.
// ProducerFn implementations should prepare new work items for the pipeline,
// then place them into the pipeline with `put`.  Production into the pipeline
// is complete when the implementation returns; if it returns a non-nil error,
// the pipeline is terminated.
type ProducerFn[T any] func(put func(T)) error

// RecyclingProducerFn performs the work of the initial stage in a pipeline,
// preparing work items of type T for processing.  The work items that pass
// through the pipeline are recycled; RecyclingProducerFn implementations
// should use `get` to recycle old work items (which may need to be reset), and
// only instantiate new work items if `get` returns false.  Implementations
// should prepare recycled or new work items for the pipeline, then place them
// into the pipeline with `put`.  Production into the pipeline is complete when
// the implementation returns; if it returns a non-nil error, the pipeline is
// terminated.
type RecyclingProducerFn[T any] func(get func() (T, bool), put func(T)) error

// StageFn performs the work of an intermediate stage in a pipeline, accepting
// a work item of T, performing some work on or with it, and then returning it
// (or another instance of T).  If it returns a non-nil error, the pipeline is
// terminated.
type StageFn[T any] func(in T) (out T, err error)

// StageOptionFn defines a user-supplied option to a Producer or a Stage.
type StageOptionFn func(so *stageOptions) error

// InputBufferSize defines the size of the input buffer of a Stage.  For
// recycling Producers, this defines the size of the input buffer of the
// recycling mechanism.  For non-recyling Producers, this has no effect.
// Defaults to 1.
func InputBufferSize(inputBufferSize uint) StageOptionFn {
	return func(so *stageOptions) error {
		if inputBufferSize == 0 {
			return fmt.Errorf("input buffer size must be at least 1")
		}
		so.inputBufferSize = inputBufferSize
		return nil
	}
}

// Name specifies the name of a Stage or a Producer, for debugging.  If
// unspecified, the Stage number (0 for the Producer) will be used.
func Name(name string) StageOptionFn {
	return func(so *stageOptions) error {
		so.name = name
		return nil
	}
}

// Concurrency specifies the desired concurrency of a Stage or Producer.
// A Stage's concurrency is the number of worker goroutines performing that
// Stage.
func Concurrency(concurrency uint) StageOptionFn {
	return func(so *stageOptions) error {
		if concurrency == 0 {
			return fmt.Errorf("concurrency must be at least 1")
		}
		so.concurrency = concurrency
		return nil
	}
}

// Producer defines a function building a pipeline producer.
type Producer[T any] func(index uint) (*producer[T], error)

// NewRecyclingProducer defines an initial stage in a pipeline, in which work
// items of type T are prepared for processing.  The provided RecyclingProducerFn
// should invoke its `get` method to get a previously-allocated work item, only
// constructing a new work item if `get` returns false.
func NewRecyclingProducer[T any](fn RecyclingProducerFn[T], optFns ...StageOptionFn) Producer[T] {
	return func(index uint) (*producer[T], error) {
		opts, err := buildStageOptions(index, optFns...)
		if err != nil {
			return nil, err
		}
		return newP(true, fn, opts), nil
	}
}

// NewProducer defines an initial stage in a pipeline, in which work items of type
// T are prepared for processing.
func NewProducer[T any](fn ProducerFn[T], optFns ...StageOptionFn) Producer[T] {
	return func(index uint) (*producer[T], error) {
		opts, err := buildStageOptions(index, optFns...)
		if err != nil {
			return nil, err
		}
		return newP(false, func(_ func() (T, bool), put func(T)) error {
			return fn(put)
		}, opts), nil
	}
}

// Stage defines a function building a pipeline stage.
type Stage[T any] func(index uint) (*stage[T], error)

// NewStage defines an intermediate stage in a pipeline, in which work items of
// type T are operated upon.
func NewStage[T any](fn StageFn[T], optFns ...StageOptionFn) Stage[T] {
	return func(index uint) (*stage[T], error) {
		opts, err := buildStageOptions(index, optFns...)
		if err != nil {
			return nil, err
		}
		return newS(fn, opts), nil
	}
}

// Do runs the parallel pipeline defined by the specified Producer and Stages.
// Work items of type T are produced by the Producer, then handled by each
// Stage in the provided order.
func Do[T any](producerDef Producer[T], stageDefs ...Stage[T]) error {
	pipeline, err := newPipeline(producerDef, stageDefs...)
	if err != nil {
		return err
	}
	return pipeline.do()
}

// Measure behaves like Do(), running the parallel pipeline defined by the
// specified Producer and Stages, but also measures the time spent in each
// stage.
func Measure[T any](producerDef Producer[T], stageDefs ...Stage[T]) (*Metrics, error) {
	pipeline, err := newPipeline(producerDef, stageDefs...)
	if err != nil {
		return nil, err
	}
	return pipeline.measure()
}

// SequentialDo behaves like Do(), but runs sequentially on one thread.
// Stage buffer lengths and concurrency options are ignored, but
// RecyclingProducers do recycle the (single) work item.
func SequentialDo[T any](producerDef Producer[T], stageDefs ...Stage[T]) error {
	pipeline, err := newPipeline(producerDef, stageDefs...)
	if err != nil {
		return err
	}
	return pipeline.sequentialDo()
}

// StageMetrics defines a set of performance metrics collected for a particular
// pipeline stage.
type StageMetrics struct {
	StageName                   string
	StageInstance               uint
	WorkDuration, StageDuration time.Duration
	Items                       uint
}

func (sm *StageMetrics) label() string {
	return fmt.Sprintf("%s (%d)", sm.StageName, sm.StageInstance)
}

func (sm *StageMetrics) detailRow(labelCols int) string {
	if sm.Items > 0 {
		formatStr := fmt.Sprintf("%%-%ds: %%d items, total %%s (%%s/item), work %%s (%%s/item)", labelCols)
		return fmt.Sprintf(formatStr,
			sm.label(),
			sm.Items,
			sm.StageDuration, sm.StageDuration/time.Duration(sm.Items),
			sm.WorkDuration, sm.WorkDuration/time.Duration(sm.Items),
		)
	} else {
		formatStr := fmt.Sprintf("%%-%ds: %%d items, total %%s, work %%s", labelCols)
		return fmt.Sprintf(formatStr,
			sm.label(),
			sm.Items,
			sm.StageDuration,
			sm.WorkDuration,
		)
	}
}

// Metrics defines a set of performance metrics collected for an
// entire pipeline.
type Metrics struct {
	WallDuration    time.Duration
	ProducerMetrics []*StageMetrics
	StageMetrics    [][]*StageMetrics
}

func (pm *Metrics) String() string {
	if pm == nil {
		return ""
	}
	labelCols := 0
	for _, producerMetrics := range pm.ProducerMetrics {
		labelLen := len(producerMetrics.label())
		if labelLen > labelCols {
			labelCols = labelLen
		}
	}
	for _, stageMetrics := range pm.StageMetrics {
		for _, stageMetric := range stageMetrics {
			labelLen := len(stageMetric.label())
			if labelLen > labelCols {
				labelCols = labelLen
			}
		}
	}
	ret := []string{fmt.Sprintf("Pipeline wall time: %s", pm.WallDuration)}
	for _, producerMetrics := range pm.ProducerMetrics {
		ret = append(ret, "  "+producerMetrics.detailRow(labelCols))
	}
	for _, stageMetrics := range pm.StageMetrics {
		for _, stageMetric := range stageMetrics {
			ret = append(ret, "  "+stageMetric.detailRow(labelCols))
		}
	}
	return strings.Join(ret, "\n")
}

type stageOptions struct {
	concurrency     uint
	inputBufferSize uint
	name            string
}

func buildStageOptions(index uint, fns ...StageOptionFn) (*stageOptions, error) {
	ret := &stageOptions{
		concurrency:     1,
		inputBufferSize: 1,
	}
	for _, fn := range fns {
		if err := fn(ret); err != nil {
			return nil, err
		}
	}
	if ret.name == "" {
		ret.name = fmt.Sprintf("stage %d", index)
	}
	return ret, nil
}

// commonStage bundles data and logic held in common among both producers and
// stages.
type commonStage[T any] struct {
	// This stage's options.
	opts *stageOptions
	// The channel from which this stage receives its input work items.  For
	// non-recycling producers, this is unused.
	inCh chan T
	// If true, the stage should output to its output channel.  True for all
	// producers, and for all stages except the last stage in non-recycling
	// pipelines.  If false, the result of the stage function is discarded.
	emitToOutCh bool
	// The channel to which this stage places its output work items.
	outCh chan<- T
}

// outputChannelCloser returns a function to be invoked when the stage is done
// producing output data.  This function closes the stage's output channel when
// invoked by the last instance of that stage to complete.
func (cs commonStage[T]) outputChannelCloser() func() {
	instances := cs.concurrency()
	var mu sync.Mutex
	return func() {
		mu.Lock()
		defer mu.Unlock()
		instances--
		if instances == 0 {
			close(cs.outCh)
		}
	}
}

func (cs commonStage[T]) concurrency() uint {
	return cs.opts.concurrency
}

func (cs commonStage[T]) name() string {
	return cs.opts.name
}

// exhaustInput consumes and discards all input work items on the stage's
// input channel.
func (cs commonStage[T]) exhaustInput() {
	for range cs.inCh {
	}
}

// producer describes the initial stage of a pipeline.
type producer[T any] struct {
	commonStage[T]
	recycling bool
	fn        RecyclingProducerFn[T]
	// If non-nil, a nonblocking function to be run by `fn` to obtain a
	// recycled work item.  Returns true iff the returned work item is valid.
	// A `false` return value indicates only that a recycled work item was not
	// available without blocking during that invocation of `getter`; subsequent
	// invocations of `getter` might succeed.
	getter func() (T, bool)
}

func newP[T any](recycling bool, fn RecyclingProducerFn[T], opts *stageOptions) *producer[T] {
	ret := &producer[T]{
		commonStage: commonStage[T]{
			opts:        opts,
			emitToOutCh: true,
			inCh:        make(chan T, opts.inputBufferSize),
		},
		recycling: recycling,
		fn:        fn,
	}
	if recycling {
		ret.getter = func() (wi T, ok bool) {
			select {
			case wi = <-ret.inCh:
				return wi, true
			default:
				return wi, false
			}
		}
	}
	return ret
}

// do invokes a single instance of the receiver's (Recycling)ProducerFn with
// the receiver's getter and a putter that writes the provided item to the
// receiver's output channel.
func (p *producer[T]) do() (err error) {
	return p.fn(p.getter, func(item T) {
		p.outCh <- item
	})
}

// measure behaves like do(), but measures the number of work items produced,
// the amount of time doing work (that is, time spent in `fn` but not in its
// calls to `get` or `put`), and the total time spent in the producer stage
// instance.
func (p *producer[T]) measure() (items uint, workDuration, stageDuration time.Duration, err error) {
	start := time.Now()
	var frameworkDuration time.Duration
	err = p.fn(func() (item T, ok bool) {
		start := time.Now()
		item, ok = p.getter()
		frameworkDuration += time.Now().Sub(start)
		return item, ok
	}, func(item T) {
		start := time.Now()
		items++
		p.outCh <- item
		frameworkDuration += time.Now().Sub(start)
	})
	stageDuration = time.Now().Sub(start)
	workDuration = stageDuration - frameworkDuration
	return items, workDuration, stageDuration, err
}

// stage describes a non-initial stage of a pipeline.
type stage[T any] struct {
	commonStage[T]
	fn StageFn[T]
}

func newS[T any](fn StageFn[T], opts *stageOptions) *stage[T] {
	ret := &stage[T]{
		commonStage: commonStage[T]{
			opts:        opts,
			emitToOutCh: true,
			inCh:        make(chan T, opts.inputBufferSize),
		},
		fn: fn,
	}
	return ret
}

// doOne performs the receiving stage's work on a single work item: it fetches
// an input work item from its input channel, works on it, and, if enabled,
// places the work result in its output channel.
// doOne returns false (and outputs nothing) if there is no further input on
// the input channel.
func (s *stage[T]) doOne() (ok bool, err error) {
	var in, out T
	in, ok = <-s.inCh
	if !ok {
		return false, nil
	}
	out, err = s.fn(in)
	if err == nil && s.emitToOutCh {
		s.outCh <- out
	}
	return ok, err
}

// do invokes one instance of the receiving stage, fetching work items, working
// on them, and passing the results on to the next stage until no more input is
// available.
func (s *stage[T]) do() (err error) {
	ok, err := s.doOne()
	for ok && err == nil {
		ok, err = s.doOne()
	}
	return err
}

// measureOne behaves like doOne(), but additionally tracks and returns the
// time spent working (that is, within `fn`).
func (s *stage[T]) measureOne() (ok bool, workDuration time.Duration, err error) {
	var in, out T
	in, ok = <-s.inCh
	if !ok {
		return false, 0, nil
	}
	var dur time.Duration
	start := time.Now()
	out, err = s.fn(in)
	dur = time.Now().Sub(start)
	if err == nil && s.emitToOutCh {
		s.outCh <- out
	}
	return ok, dur, err
}

// measure behaves like do(), but additionally tracks and returns the number of
// items processed, the amount of time doing work (that is, time spent in `fn`,
// and the total time spent in the stage instance.
func (s *stage[T]) measure() (items uint, workDuration, stageDuration time.Duration, err error) {
	start := time.Now()
	items++
	ok, workDur, err := s.measureOne()
	workDuration += workDur
	for ok && err == nil {
		ok, workDur, err = s.measureOne()
		if ok && err == nil {
			items++
		}
		workDuration += workDur
	}
	stageDuration = time.Now().Sub(start)
	return items, workDuration, stageDuration, err
}

// pipeline facilitates the construction and use of a complete dataflow
// pipeline.
type pipeline[T any] struct {
	producer *producer[T]
	stages   []*stage[T]
}

func newPipeline[T any](producerDef Producer[T], stageDefs ...Stage[T]) (*pipeline[T], error) {
	if len(stageDefs) == 0 {
		return nil, fmt.Errorf("pipeline must have a producer and at least one stage")
	}
	ret := &pipeline[T]{}
	var err error
	// Prepare an input buffer for the producer and for each stage.
	ret.producer, err = producerDef(0)
	if err != nil {
		return nil, err
	}
	ret.stages = make([]*stage[T], len(stageDefs))
	for idx, stageDef := range stageDefs {
		stage, err := stageDef(uint(idx) + 1)
		if err != nil {
			return nil, err
		}
		ret.stages[idx] = stage
	}
	// Set each stage's input and output channels. (including the producer's) to the next stage's
	// inCh.
	ret.producer.outCh = ret.stages[0].inCh
	lastStage := ret.stages[0]
	for _, stage := range ret.stages[1:] {
		lastStage.outCh = stage.inCh
		lastStage = stage
	}
	lastStage.outCh = ret.producer.inCh
	if !ret.producer.recycling {
		lastStage.emitToOutCh = false
	}
	return ret, nil
}

type doer interface {
	outputChannelCloser() func()
	concurrency() uint
	name() string
	exhaustInput()
	do() error
	measure() (items uint, workDuration, stageDuration time.Duration, err error)
}

// Runs all instances of the provided `doer` (`producer` or `stage`) as
// goroutines using the provided errorgroup.
func do(eg *errgroup.Group, doer doer) {
	closeOutputChannel := doer.outputChannelCloser()
	for i := uint(0); i < doer.concurrency(); i++ {
		eg.Go(func() error {
			// For producers, produce all work items.  For stages, process inputs
			// until there are no more.
			err := doer.do()
			// Close the output channel, signaling to the downstream stage that no
			// more input is coming.
			closeOutputChannel()
			// Exhaust all remaining input on the input channel to unblock any
			// goroutines writing to them.  This is necessary for recycling producers
			// or in the case of an error in the pipeline.
			doer.exhaustInput()
			return err
		})
	}
}

// Like do(), but tracking metrics and returning a StageMetrics for each
// instance.  The returned StageMetrics should not be examined until after
// eg.Wait().
func measure(eg *errgroup.Group, doer doer) []*StageMetrics {
	ret := make([]*StageMetrics, doer.concurrency())
	closeOutputChannel := doer.outputChannelCloser()
	for i := uint(0); i < doer.concurrency(); i++ {
		index := i
		ret[index] = &StageMetrics{
			StageName:     doer.name(),
			StageInstance: index,
		}
		eg.Go(func() error {
			var err error
			ret[index].Items, ret[index].WorkDuration, ret[index].StageDuration, err = doer.measure()
			start := time.Now()
			closeOutputChannel()
			doer.exhaustInput()
			ret[index].StageDuration += time.Now().Sub(start)
			return err
		})
	}
	return ret
}

// do executes the pipeline in parallel.
func (p *pipeline[T]) do() error {
	var eg errgroup.Group
	do(&eg, p.producer)
	for _, stage := range p.stages {
		do(&eg, stage)
	}
	return eg.Wait()
}

// sequentialDo is like do(), but executes the pipeline in serial for each
// produced work item, and doesn't use channels.
func (p *pipeline[T]) sequentialDo() error {
	var err error
	itemAvailable := false
	var pendingItem T
	producerErr := p.producer.fn(
		func() (T, bool) {
			return pendingItem, itemAvailable
		},
		func(item T) {
			for _, stage := range p.stages {
				item, err = stage.fn(item)
			}
			if p.producer.recycling {
				itemAvailable = true
				pendingItem = item
			}
		})
	if producerErr != nil {
		return producerErr
	}
	return err
}

// measure is like do, but measures performance and returns pipeline Metrics.
func (p *pipeline[T]) measure() (*Metrics, error) {
	ret := &Metrics{}
	start := time.Now()
	var eg errgroup.Group
	producerMetrics := measure(&eg, p.producer)
	stageMetrics := make([][]*StageMetrics, len(p.stages))
	for idx, stage := range p.stages {
		stageMetrics[idx] = measure(&eg, stage)
	}
	err := eg.Wait()
	ret.WallDuration = time.Now().Sub(start)
	ret.ProducerMetrics = producerMetrics
	ret.StageMetrics = stageMetrics
	return ret, err
}
