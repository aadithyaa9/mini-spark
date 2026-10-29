package rdds

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/aadithyaa9/mini-spark/pkg/core"
)

func TestTextFileSplitBoundaries(t *testing.T) {
	path := filepath.Join(t.TempDir(), "lines.txt")
	var sb strings.Builder
	const numLines = 5000
	for i := 0; i < numLines; i++ {
		fmt.Fprintf(&sb, "line-%05d with some padding to make splits land mid-line\n", i)
	}
	if err := os.WriteFile(path, []byte(sb.String()), 0o644); err != nil {
		t.Fatal(err)
	}

	for _, parts := range []int{1, 2, 3, 7, 16, 64} {
		r, err := NewTextFile(path, parts)
		if err != nil {
			t.Fatal(err)
		}
		seen := map[string]int{}
		for _, p := range r.Partitions() {
			it, err := r.Compute(p, nil)
			if err != nil {
				t.Fatal(err)
			}
			recs, err := core.Collect(it)
			if err != nil {
				t.Fatal(err)
			}
			for _, rec := range recs {
				seen[rec.(string)]++
			}
		}
		if len(seen) != numLines {
			t.Fatalf("partitions=%d: %d distinct lines, want %d", parts, len(seen), numLines)
		}
		for line, n := range seen {
			if n != 1 {
				t.Fatalf("partitions=%d: %q read %d times", parts, line, n)
			}
		}
	}
}