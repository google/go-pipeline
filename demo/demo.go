// Binary demo is a demonstration of go-pipeline.
//
// The input dictionary file should be a plain text file with one word per lin.
// E.g., via `curl -o words.txt https://raw.githubusercontent.com/dwyl/english-words/a77cb15f4f5beb59c15b945f2415328a6b33c3b0/words.txt`.
package main

import (
	"flag"
	"log"
	"os"
	"strings"
	"time"

	"github.com/google/go-pipeline/pkg/pipeline"
	"github.com/google/go-pipeline/prefixtree"
)

var mode = flag.String("mode", "serial", `Tree construction mode:
  'serial' to compute the tree in serial,
  'concurrent' to compute the tree in parallel via a pipeline,
  'measure' as 'concurrent', but reporting pipeline performance.`)
var dictFilename = flag.String("dict_filename", "",
	"The filename of a dictionary from which to construct a dictionary.")
var batchSize = flag.Int("batch_size", 5000, "The size of a batch in parallel processing")

type batch struct {
	root  *prefixtree.Node
	words []string
}

func main() {
	flag.Parse()

	dict, err := os.ReadFile(*dictFilename)
	if err != nil {
		log.Fatalf("Failed to load dictionary file '%s': %s", *dictFilename, err)
	}
	words := strings.Split(string(dict), "\n")

	root := prefixtree.New()
	start := time.Now()
	defer func() {
		log.Printf("Prefix tree construction took %s.  Added %d words.", time.Now().Sub(start), root.Count)
	}()
	switch *mode {
	case "serial":
		for _, word := range words {
			root.Insert(word)
		}
	case "concurrent", "measure":
		// Define a recycling producer function to batch up the dict..
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
		// Define a stage function that builds a prefix tree from this
		// batch.
		localInsertFn := func(in *batch) (out *batch, err error) {
			for _, word := range in.words {
				in.root.Insert(word)
			}
			return in, nil
		}
		// Define a stage function that merges a batch's prefix tree
		// into the global prefix tree.
		globalMergeFn := func(in *batch) (out *batch, err error) {
			root.MergeFrom(in.root)
			return in, nil
		}
		// Set up the pipeline's producer and stages
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
		// Then invoke the pipeline.
		if *mode == "concurrent" {
			if err := pipeline.Do(
				producer,
				localInsertStage,
				globalMergeStage,
			); err != nil {
				log.Fatalf("Do() failed: %s", err)
			}
		} else {
			if stats, err := pipeline.Measure(
				producer,
				localInsertStage,
				globalMergeStage,
			); err != nil {
				log.Fatalf("Measure() failed: %s", err)
			} else {
				log.Printf(stats.String())
			}
		}
	default:
		log.Fatalf("Unsupported mode '%s'", *mode)
	}

}
