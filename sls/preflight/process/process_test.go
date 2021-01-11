package process

import (
	"fmt"
	"io/ioutil"
	"strings"
	"testing"
	"time"

	"github.com/a-h/ddbimport/sls/state"
	"github.com/google/go-cmp/cmp"
	"go.uber.org/zap"
)

func TestProcess(t *testing.T) {
	var tests = []struct {
		rowCount        int
		batchSize       int64
		expectedBatches [][]int64
	}{
		{
			rowCount:  0,
			batchSize: 1,
			expectedBatches: [][]int64{
				{0, 6}, // Just the header.
			},
		},
		{
			rowCount:  1,
			batchSize: 1,
			expectedBatches: [][]int64{
				{0, 6},  // Header.
				{6, 12}, // First row.
			},
		},
		{
			rowCount:  2,
			batchSize: 2,
			expectedBatches: [][]int64{
				{0, 12},  // Header and first row.
				{12, 18}, // Remainder.
			},
		},
		{
			rowCount:  4,
			batchSize: 3,
			expectedBatches: [][]int64{
				{0, 18},  // Header and first 2 rows (6 bytes * 3 rows).
				{18, 30}, // Remainder (6 bytes * 2 rows).
			},
		},
	}
	for _, tt := range tests {
		tt := tt
		name := fmt.Sprintf("%d rows in batches of %d", tt.rowCount, tt.batchSize)
		t.Run(name, func(t *testing.T) {
			src := generate(tt.rowCount)
			size := len(src)
			rdr := ioutil.NopCloser(strings.NewReader(src))
			var req state.State
			req.Source.Delimiter = ","
			req.Configuration.LambdaDurationSeconds = 500
			resp, err := Process(zap.New(nil), time.Now, rdr, int64(size), tt.batchSize, req)
			if err != nil {
				t.Error(err)
				return
			}
			if diff := cmp.Diff(tt.expectedBatches, resp.Batches); diff != "" {
				t.Errorf(diff)
			}
		})
	}
}

func generate(n int) string {
	var sb strings.Builder
	sb.WriteString("a,b,c\n")
	for i := 0; i < n; i++ {
		sb.WriteString("x,y,z\n")
	}
	return sb.String()
}
