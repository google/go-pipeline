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

package pipeline

import (
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
)

func StringProducer(strs ...string) Producer[string] {
	return NewProducer(func(put func(string)) error {
		for _, str := range strs {
			put(str)
		}
		return nil
	}, Name("string producer"))
}

var ToUpper = NewStage(func(in string) (string, error) {
	return strings.ToUpper(in), nil
}, Name("to upper"))

var Double = NewStage(func(in string) (string, error) {
	return in + in, nil
}, Name("double"))

var First = NewStage(func(in string) (string, error) {
	return strings.Split(in, " ")[0], nil
}, Name("first"))

func Oops[T any](after int) Stage[T] {
	return NewStage(func(in T) (T, error) {
		if after == 0 {
			return in, fmt.Errorf("oops")
		}
		after--
		return in, nil
	})
}

func TestNonrecyclingPipeline(t *testing.T) {
	for _, test := range []struct {
		description string
		invoke      func(...string) ([]string, error)
	}{{
		description: "Do()",
		invoke: func(names ...string) ([]string, error) {
			gotStrs := []string{}
			err := Do(
				StringProducer(names...),
				First,
				Double,
				ToUpper,
				NewStage(func(in string) (string, error) {
					gotStrs = append(gotStrs, in)
					return "", nil
				}))
			return gotStrs, err
		},
	}, {
		description: "Measure()",
		invoke: func(names ...string) ([]string, error) {
			gotStrs := []string{}
			m, err := Measure(
				StringProducer(names...),
				First,
				Double,
				ToUpper,
				NewStage(func(in string) (string, error) {
					gotStrs = append(gotStrs, in)
					return "", nil
				}))
			if err != nil {
				return nil, err
			}
			if m.ProducerMetrics[0].Items != uint(len(names)) {
				return nil, fmt.Errorf("%d items produced, expected %d", m.ProducerMetrics[0].Items, len(names))
			}
			for _, sm := range m.StageMetrics {
				if sm[0].Items != uint(len(names)) {
					return nil, fmt.Errorf("Stage %s processed %d items, expected %d", sm[0].StageName, sm[0].Items, len(names))
				}
			}
			return gotStrs, err
		},
	}, {
		description: "SequentialDo()",
		invoke: func(names ...string) ([]string, error) {
			gotStrs := []string{}
			err := SequentialDo(
				StringProducer(names...),
				First,
				Double,
				ToUpper,
				NewStage(func(in string) (string, error) {
					gotStrs = append(gotStrs, in)
					return "", nil
				}))
			return gotStrs, err
		},
	}} {
		t.Run(test.description, func(t *testing.T) {
			gotStrs, err := test.invoke("Nick Name", "Elmer Fudd", "Jane Doe")
			if err != nil {
				t.Errorf("pipeline yielded %v, wanted nil", err)
			}
			wantStrs := []string{"NICKNICK", "ELMERELMER", "JANEJANE"}
			if diff := cmp.Diff(wantStrs, gotStrs); diff != "" {
				t.Errorf("Pipeline produced %v, diff (-want +got) %s", gotStrs, diff)
			}
		})
	}
}

func TestNonrecyclingPipelineError(t *testing.T) {
	gotPostError := 0
	err := Do(
		StringProducer("Nick Name", "Elmer Fudd", "Jane Doe"),
		First,
		Double,
		Oops[string](2),
		NewStage(func(in string) (string, error) {
			gotPostError++
			return in, nil
		}),
		ToUpper)
	if err == nil {
		t.Errorf("Do() = nil, wanted non-nil")
	}
	if gotPostError != 2 {
		t.Errorf("Do() failed after %d messages, wanted 2", gotPostError)
	}
}

type fruit struct {
	name     string
	color    string
	quantity int
}

func (f *fruit) fromString(fStr string) error {
	parts := strings.Split(fStr, "/")
	f.name = parts[0]
	f.color = parts[1]
	quant, err := strconv.Atoi(parts[2])
	if err != nil {
		return err
	}
	f.quantity = quant
	return nil
}

func FruitProducer(incCreated, incRecycled func(), strs ...string) Producer[*fruit] {
	return NewRecyclingProducer(func(get func() (*fruit, bool), put func(*fruit)) error {
		for _, str := range strs {
			f, ok := get()
			if !ok {
				if incCreated != nil {
					incCreated()
				}
				f = &fruit{}
			} else {
				if incRecycled != nil {
					incRecycled()
				}
			}
			if err := f.fromString(str); err != nil {
				return err
			}
			put(f)
		}
		return nil
	}, Name("fruit producer"))
}

func TallyFruitName(names map[string]int) Stage[*fruit] {
	return NewStage(func(in *fruit) (*fruit, error) {
		names[in.name] += in.quantity
		return in, nil
	}, Name("tally names"))
}

func TallyFruitColors(colors map[string]int) Stage[*fruit] {
	return NewStage(func(in *fruit) (*fruit, error) {
		colors[in.color] += in.quantity
		return in, nil
	}, Name("tally colors"))
}

func TestRecyclingPipeline(t *testing.T) {
	for _, test := range []struct {
		description string
		invoke      func(names, colors map[string]int, fruitStrs ...string) error
	}{{
		description: "Do()",
		invoke: func(names, colors map[string]int, fruitStrs ...string) error {
			return Do(
				FruitProducer(nil, nil, fruitStrs...),
				TallyFruitName(names),
				TallyFruitColors(colors),
			)
		},
	}, {
		description: "Measure()",
		invoke: func(names, colors map[string]int, fruitStrs ...string) error {
			created, recycled := 0, 0
			_, err := Measure(
				FruitProducer(func() { created++ }, func() { recycled++ }, fruitStrs...),
				TallyFruitName(names),
				TallyFruitColors(colors),
			)
			if err != nil {
				return err
			}
			if recycled == 0 {
				return fmt.Errorf("expected at least one *fruit to be recycled")
			}
			if created+recycled != len(fruitStrs) {
				return fmt.Errorf("%d *fruits created or recycled, expected %d", created+recycled, len(fruitStrs))
			}
			return nil
		},
	}, {
		description: "SequentialDo()",
		invoke: func(names, colors map[string]int, fruitStrs ...string) error {
			return SequentialDo(
				FruitProducer(nil, nil, fruitStrs...),
				TallyFruitName(names),
				TallyFruitColors(colors),
			)
		},
	}} {
		t.Run(test.description, func(t *testing.T) {
			gotNames := map[string]int{}
			gotColors := map[string]int{}
			err := test.invoke(gotNames, gotColors,
				"apple/red/3",
				"apple/green/2",
				"pear/brown/1",
				"pear/green/2",
				"grape/red/5",
				"grape/green/1")
			if err != nil {
				t.Errorf("pipeline yielded '%v', wanted nil", err)
			}
			if diff := cmp.Diff(map[string]int{
				"apple": 5,
				"pear":  3,
				"grape": 6,
			}, gotNames); diff != "" {
				t.Errorf("Names aggregated to %v, diff (-want +got) %s", gotNames, diff)
			}
			if diff := cmp.Diff(map[string]int{
				"red":   8,
				"green": 5,
				"brown": 1,
			}, gotColors); diff != "" {
				t.Errorf("Colors aggregated to %v, diff (-want +got) %s", gotColors, diff)
			}
		})
	}
}

func TestRecyclingPipelineError(t *testing.T) {
	names := map[string]int{}
	colors := map[string]int{}
	m, err := Measure(
		FruitProducer(
			nil,
			nil,
			"apple/red/3",
			"apple/green/2",
			"starfruit/yellow/five", // Atoi won't like 'five'.
			"pear/brown/1",
			"pear/green/2",
			"grape/red/5",
			"grape/green/1",
		),
		TallyFruitName(names),
		TallyFruitColors(colors),
	)
	if err == nil {
		t.Errorf("pipeline yielded nil error, wanted non-nil")
	}
	var wantProduced uint = 2
	gotProduced := m.ProducerMetrics[0].Items
	if gotProduced != wantProduced {
		t.Errorf("Produced %d items, wanted %d", gotProduced, wantProduced)
	}
}
