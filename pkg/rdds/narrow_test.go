package rdds

import (
	"testing"

	"github.com/aadithyaa9/mini-spark/pkg/core"
)

func TestParallelCollectionKeepsData(t *testing.T) {
	r := NewParallelCollectionRDD([]any{1, 2, 3, 4, 5, 6, 7}, 3)
	total := 0
	for _, p := range r.Partitions() {
		it, err := r.Compute(p, nil)
		if err != nil {
			t.Fatal(err)
		}
		recs, _ := core.Collect(it)
		total += len(recs)
	}
	if total != 7 {
		t.Fatalf("got %d records, want 7", total)
	}
}

func TestUnionHasNoNilDependencies(t *testing.T) {
	a := NewParallelCollectionRDD([]any{1, 2}, 2)
	b := NewParallelCollectionRDD([]any{3}, 1)
	u := NewUnionRDD([]core.RDD{a, b})

	if got := len(u.Dependencies()); got != 2 {
		t.Fatalf("got %d dependencies, want 2", got)
	}
	if got := len(u.Partitions()); got != 3 {
		t.Fatalf("got %d partitions, want 3", got)
	}
	core.Lineage(u) // panics if any dependency is nil
}
