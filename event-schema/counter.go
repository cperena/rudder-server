package event_schema

import (
	"encoding/json"

	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/event-schema/analytics"
)

type CounterTypeT string

const (
	LossyCount    CounterTypeT = "LossyCount"
	StickySampler CounterTypeT = "StickySamplerT"
)

var defaultCounterType CounterTypeT
var counterSupport, counterErrorTolerance, counterFailureProb, counterThreshold float64

type FrequencyCounter struct {
	Name        string
	CounterType CounterTypeT
	Counter     interface{}
}

func (fc *FrequencyCounter) getCounter() analytics.Counter {
	switch fc.CounterType {
	case LossyCount:
		return fc.Counter.(*analytics.LossyCounterT)
	case StickySampler:
		return fc.Counter.(*analytics.StickySamplerT)
	default:
		panic("Unexpected countertype") //TODO: Handle it in a better way
	}
}

func (fc *FrequencyCounter) setCounter(counterType CounterTypeT, counter analytics.Counter) {
	fc.Counter = counter
	fc.CounterType = counterType
}

func init() {
	counterTypeStr := config.GetString("EventSchemas.counterType", "LossyCount")

	// Output every elem has appeared at least (Counter * support) times
	counterSupport = config.GetFloat64("EventSchemas.counterSupport", 0.01)

	// We can start with support/10
	counterErrorTolerance = config.GetFloat64("EventSchemas.counterErrorTolerance", 0.001)

	//
	counterFailureProb = config.GetFloat64("EventSchemas.counterFailureProb", 0.01)

	// Check this?
	counterThreshold = config.GetFloat64("EventSchemas.counterThreshold", 0.01)

	if counterTypeStr == string(StickySampler) {
		defaultCounterType = StickySampler
	} else {
		defaultCounterType = LossyCount
	}

}

func NewFrequencyCounter(name string) *FrequencyCounter {
	fc := FrequencyCounter{}
	fc.Name = name
	var counter analytics.Counter
	if defaultCounterType == LossyCount {
		counter = analytics.NewLossyCounter(counterSupport, counterErrorTolerance)
	} else {
		counter = analytics.NewSampler(counterSupport, counterErrorTolerance, counterFailureProb)
	}
	fc.setCounter(defaultCounterType, counter)
	return &fc
}

func NewPeristedFrequencyCounter(persistedFc *FrequencyCounter) *FrequencyCounter {
	fc := FrequencyCounter{}
	fc.Name = persistedFc.Name
	var cType CounterTypeT
	var counter analytics.Counter

	if persistedFc.CounterType == LossyCount {
		var lc analytics.LossyCounterT
		persistedFcJSON, _ := json.Marshal(persistedFc.Counter)
		err := json.Unmarshal(persistedFcJSON, &lc)
		if err != nil {
			panic(err)
		}
		counter = analytics.Counter(&lc)
		cType = LossyCount
	} else {
		var ss analytics.StickySamplerT
		persistedFcJSON, _ := json.Marshal(persistedFc.Counter)
		err := json.Unmarshal(persistedFcJSON, &ss)
		if err != nil {
			panic(err)
		}
		counter = analytics.Counter(&ss)
		cType = StickySampler
	}

	fc.setCounter(cType, counter)
	return &fc
}

func (fc *FrequencyCounter) Observe(key string) {
	fc.getCounter().Observe(key)
}

// If we add counter support per key, change accordingly
func getCounterSupport(key string) float64 {
	return counterSupport
}
func (fc *FrequencyCounter) ItemsAboveThreshold() []analytics.EntryT {
	return fc.getCounter().ItemsAboveThreshold(counterThreshold)
}
