package wal

import (
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
)

// ns/op here is a whole commit group, not one player's request latency.
func BenchmarkWALAppend(b *testing.B) {
	for _, size := range []int{1, 8, 64} {
		b.Run(fmt.Sprintf("batch%d", size), func(b *testing.B) {
			log, err := Open(filepath.Join(b.TempDir(), "append.wal"))
			if err != nil {
				b.Fatal(err)
			}
			defer log.Close()
			payload := json.RawMessage(`{"event":"reward","padding":"` + strings.Repeat("x", 450) + `"}`)
			group := make([]json.RawMessage, size)
			for i := range group {
				group[i] = payload
			}
			b.ReportAllocs()
			b.SetBytes(int64(len(payload) * size))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := log.AppendBatch(group); err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*size), "ns/record")
		})
	}
}
