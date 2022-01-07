package quantile

import (
	"math/rand"
	"time"

	"testing"

	"github.com/prometheus/client_golang/prometheus"
)

var (
	labels = []string{"label-some1", "label-some2", "label-some3", "label-some4", "label-some5", "label-some6"}
)

func Benchmark_Summary(b *testing.B) {
	rndValues := rand.New(rand.NewSource(10001))
	rndLabels := rand.New(rand.NewSource(3))

	temps := prometheus.NewSummaryVec(
		prometheus.SummaryOpts{
			Name:       "task_resolving_duration",
			Help:       "Tasks duration.",
			Objectives: map[float64]float64{0.5: 0.05, 0.95: 0.01, 0.99: 0.001},
			MaxAge:     time.Hour,
		},
		[]string{"label"},
	)

	reg := prometheus.NewRegistry()
	reg.MustRegister(temps)

	b.ResetTimer()

	// coef := math.Pow(5*24*60*60, 1)
	for i := 0; i < b.N; i++ {
		// v := math.Pow(coef*rndValues.Float64(), 1)
		temps.WithLabelValues(labels[rndLabels.Int()%len(labels)]).Observe(60.0 + 5*24*60*60*rndValues.Float64())

		// Create a Summary without any observations.
		// temps.WithLabelValues("leiopelma-hochstetteri")

		if i%100 == 0 {
			metricFamilies, err := reg.Gather()
			if err != nil || len(metricFamilies) != 1 {
				panic("unexpected behavior of custom test registry")
			}
			// b.Log(proto.MarshalTextString(metricFamilies[0]))
		}
	}
	// reg := prometheus.NewRegistry()
	// reg.MustRegister(temps)
	// metricFamilies, err := reg.Gather()
	// if err != nil || len(metricFamilies) != 1 {
	// 	panic("unexpected behavior of custom test registry")
	// }
	// b.Log(proto.MarshalTextString(metricFamilies[0]))
}
