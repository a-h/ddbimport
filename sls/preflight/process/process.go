package process

import (
	"encoding/csv"
	"io"
	"time"

	"github.com/a-h/ddbimport/sls/linereader"
	"github.com/a-h/ddbimport/sls/state"
	"go.uber.org/zap"
)

func Process(logger *zap.Logger, now func() time.Time, src io.ReadCloser, srcSize int64, batchSize int64, req state.State) (resp state.State, err error) {
	start := now()
	resp = req
	maxDuration := req.Configuration.LambdaDurationSeconds * time.Second

	// Parse the CSV data, keeping track of the byte position in the file.
	lines := resp.Preflight.Line
	batchStartIndex := req.Preflight.Offset
	lr := linereader.New(src, resp.Preflight.Line, resp.Preflight.Offset, func(line, offset int64) {
		lines++
		resp.Preflight.Line = line
		resp.Preflight.Offset = offset
		if lines%batchSize == 0 {
			resp.Batches = append(resp.Batches, []int64{batchStartIndex, offset})
			batchStartIndex = offset
		}
	})

	csvr := csv.NewReader(lr)
	csvr.Comma = rune(resp.Source.Delimiter[0])
	var timedOut bool
	var recordCount int64
	for {
		var record []string
		record, err = csvr.Read()
		if err != nil && err != io.EOF {
			return
		}
		resp.Preflight.Continue = err == nil
		if resp.Preflight.Columns == nil {
			resp.Preflight.Columns = record
		}
		if now().Sub(start) > maxDuration {
			timedOut = true
			break
		}
		if err == io.EOF {
			err = nil
			break
		}
		recordCount++
		if recordCount%50000 == 0 {
			logger.Info("progress update", zap.Int64("records", recordCount))
		}
	}
	logger = logger.With(zap.Int64("records", recordCount))
	if batchStartIndex != lr.Offset {
		resp.Batches = append(resp.Batches, []int64{batchStartIndex, lr.Offset})
	}
	resp.Preflight.Offset = lr.Offset
	if timedOut {
		logger.Info("continuing", zap.Int64("nextStartOffset", resp.Preflight.Offset))
		return
	}
	logger.Info("complete")
	return
}
