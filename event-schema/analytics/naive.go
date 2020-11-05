package analytics

type Counter interface {
	Observe(string)
	ItemsAboveThreshold(float64) []EntryT
}

type naiveSamplerT struct {
	vals    map[string]uint64
	Counter uint64
}

func NewNaiveSampler() *naiveSamplerT {
	return &naiveSamplerT{
		vals: make(map[string]uint64),
	}
}

func (ns *naiveSamplerT) Observe(key string) {
	ns.vals[key]++
	ns.Counter++
}

func (ns *naiveSamplerT) ItemsAboveThreshold(val float64) []EntryT {
	count := uint64(val * float64(ns.Counter))
	var entries []EntryT
	for key, val := range ns.vals {
		if val >= count {
			entries = append(entries, EntryT{Key: key, Frequency: float64(val) / float64(ns.Counter)})
		}
	}
	return entries
}
