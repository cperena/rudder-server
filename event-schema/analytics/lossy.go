package analytics

import (
	"math"
)

type FeatureDeltaPair struct {
	FeatureValue float64
	Delta        float64
}

type LossyCounterT struct {
	Support        float64
	ErrorTolerance float64
	DeltaMap       map[string]FeatureDeltaPair
	Counter        uint64
	BucketWidth    uint64
}

func NewLossyCounter(support, errorTolerance float64) *LossyCounterT {
	return &LossyCounterT{
		Support:        support,
		ErrorTolerance: errorTolerance,
		DeltaMap:       make(map[string]FeatureDeltaPair),
		BucketWidth:    uint64(math.Ceil(1 / errorTolerance)),
		Counter:        0,
	}
}
func (lc *LossyCounterT) prune(bucket uint64) {
	fbucket := float64(bucket)
	for key, value := range lc.DeltaMap {
		if value.FeatureValue+value.Delta <= fbucket {
			delete(lc.DeltaMap, key)
		}
	}
}

// ItemsAboveThreshold returns a list of items that occur more than threshold, along
// with their frequencies. threshold is in the range [0,1]
func (lc *LossyCounterT) ItemsAboveThreshold(threshold float64) []EntryT {
	var results []EntryT
	counter := float64(lc.Counter)
	for key, val := range lc.DeltaMap {
		if val.FeatureValue >= (threshold-float64(lc.ErrorTolerance))*counter {
			results = append(results, EntryT{Key: key, Frequency: val.FeatureValue/counter + lc.Support})
		}
	}
	return results
}

// Observe records a new sample
func (lc *LossyCounterT) Observe(key string) {
	lc.Counter++
	bucketNo := lc.Counter / lc.BucketWidth
	val, exists := lc.DeltaMap[key]
	if exists {
		val.FeatureValue++
	} else {
		// reuse 0 val from lookup.
		val.FeatureValue = 1
		val.Delta = float64(bucketNo - 1) // this doesn't make much sense
	}
	lc.DeltaMap[key] = val
	if lc.Counter%lc.BucketWidth == 0 {
		lc.prune(bucketNo)
	}
}
