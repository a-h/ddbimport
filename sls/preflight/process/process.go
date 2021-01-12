package process

import (
	"encoding/csv"
	"io"

	"github.com/a-h/ddbimport/sls/linereader"
	"github.com/a-h/ddbimport/sls/state"
	"go.uber.org/zap"
)

func Process(logger *zap.Logger, hasTimedOut func() bool, src io.ReadCloser, srcSize int64, batchSize int64, req state.State) (resp state.State, err error) {
	resp = req

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
	var recordCount int64
	for {
		var record []string
		record, err = csvr.Read()
		if err != nil && err != io.EOF {
			return
		}
		recordCount++
		if recordCount%50000 == 0 {
			logger.Info("progress update", zap.Int64("records", recordCount))
		}
		if resp.Preflight.Columns == nil {
			resp.Preflight.Columns = record
		}
		if err == io.EOF {
			// Add trailing records.
			if batchStartIndex != lr.Offset {
				resp.Batches = append(resp.Batches, []int64{batchStartIndex, lr.Offset})
			}
			// Stop reading, start processing.
			resp.Preflight.Continue = false
			err = nil
			logger.Info("complete", zap.Int64("records", recordCount))
			return
		}
		if hasTimedOut() {
			resp.Preflight.Offset = batchStartIndex // Carry on from the start of the current batch.
			resp.Preflight.Continue = true          // There is more to process, we didn't reach EOF.
			logger.Info("continuing", zap.Int64("nextStartOffset", resp.Preflight.Offset))
			return
		}
	}
}
