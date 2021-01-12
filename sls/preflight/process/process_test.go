package process

import (
	"fmt"
	"io/ioutil"
	"strings"
	"testing"

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
			hasTimedOut := func() bool { return false }
			resp, err := Process(zap.New(nil), hasTimedOut, rdr, int64(size), tt.batchSize, req)
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

func TestProcessTimeout(t *testing.T) {
	var tests = []struct {
		name               string
		rowCount           int
		batchSize          int64
		timeOutAfterNRows  int
		expectedBatches    [][]int64
		expectedContinue   bool
		expectedFromOffset int64
	}{
		{
			name:              "timing out results in telling the step function to continue",
			rowCount:          100,
			batchSize:         2,
			timeOutAfterNRows: 2, // Including header.
			expectedBatches: [][]int64{
				{0, 12}, // Headers and first row.
			},
			expectedContinue:   true,
			expectedFromOffset: 12,
		},
		{
			name:              "timing out mid-batch results in starting from the end of the last batch",
			rowCount:          100,
			batchSize:         2,
			timeOutAfterNRows: 3,
			expectedBatches: [][]int64{
				{0, 12}, // Headers and first row. A single batch got processed.
			},
			expectedContinue:   true,
			expectedFromOffset: 12,
		},
		{
			name:              "when the timeout is at the same time as the EOF, EOF wins",
			rowCount:          3,
			batchSize:         2,
			timeOutAfterNRows: 4,
			expectedBatches: [][]int64{
				{0, 12},
				{12, 24},
			},
			expectedContinue:   true,
			expectedFromOffset: 24,
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			src := generate(tt.rowCount)
			size := len(src)
			rdr := ioutil.NopCloser(strings.NewReader(src))
			var req state.State
			req.Source.Delimiter = ","
			req.Configuration.LambdaDurationSeconds = 500
			var rowCount int
			hasTimedOut := func() bool {
				rowCount++
				return rowCount >= tt.timeOutAfterNRows
			}
			resp, err := Process(zap.New(nil), hasTimedOut, rdr, int64(size), tt.batchSize, req)
			if err != nil {
				t.Error(err)
				return
			}
			if diff := cmp.Diff(tt.expectedBatches, resp.Batches); diff != "" {
				t.Errorf(diff)
			}
			if resp.Preflight.Continue != tt.expectedContinue {
				t.Errorf("expected continue %v, got %v", tt.expectedContinue, resp.Preflight.Continue)
			}
			if resp.Preflight.Offset != tt.expectedFromOffset {
				t.Errorf("expected continue from %v, got %v", tt.expectedFromOffset, resp.Preflight.Offset)
				fmt.Println(len(src))
			}
		})
	}
}
