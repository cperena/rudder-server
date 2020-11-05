package analytics

import (
	"math"
	"math/rand"
)

var RandomFloat = rand.Float64
var RandomCoin = rand.Int31n

type StickySamplerT struct {
	ErrorTolerance  float64
	Support         float64
	ValueMap        map[string]float64
	R               float64
	FailureProb     float64
	Counter         float64
	T               float64
	RequiredSamples int
}

func NewSampler(Support, ErrorTolerance, FailureProb float64) *StickySamplerT {
	twoT := 2 / ErrorTolerance * math.Log(1/(Support*FailureProb))
	return &StickySamplerT{
		ErrorTolerance:  ErrorTolerance,
		Support:         Support,
		R:               1,
		FailureProb:     FailureProb,
		T:               twoT,
		RequiredSamples: int(twoT),
		ValueMap:        make(map[string]float64),
	}
}

const sucessful = 0

func (s *StickySamplerT) prune() {
	for key, val := range s.ValueMap {
		// repeatedly toss coin
		// until coin toss is successful.
		// todo this can probably be derived
		// by looking at how close to 0
		// a number in [0, 1) is.
		for {
			if RandomCoin(2) == sucessful {
				break
			}
			// diminish by one for every
			// unsucessful outcome
			val--
			// delete if needed
			if val <= 0 {
				delete(s.ValueMap, key)
			} else {
				s.ValueMap[key] = val
			}

		}
	}
}

// ItemsAboveThreshold returns a list of items that occur more than threshold, along
// with their frequencies. threshold is in the range [0,1]
func (s *StickySamplerT) ItemsAboveThreshold(threshold float64) []EntryT {
	var results []EntryT
	for key, value := range s.ValueMap {
		if value >= (threshold-s.ErrorTolerance)*s.Counter {
			results = append(results, EntryT{Key: key, Frequency: value/s.Counter + s.Support})
		}
	}
	return results
}

// Observe records a new sample
func (s *StickySamplerT) Observe(key string) {
	s.Counter++
	count := s.Counter
	if count > s.T {
		s.T *= 2
		s.R *= 2
		s.prune()
	}
	if _, present := s.ValueMap[key]; !present {
		// determine if value should be sampled
		shouldSample := RandomFloat() <= 1/s.R
		if !shouldSample {
			return
		}
	}
	s.ValueMap[key]++
	return
}
