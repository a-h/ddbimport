package main

import (
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/a-h/ddbimport/batchwriter"
	"github.com/a-h/ddbimport/csvtodynamo"
	"github.com/a-h/ddbimport/log"
	"github.com/a-h/ddbimport/sls/state"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/s3"
	"go.uber.org/zap"
)

// Response from the Lambda.
type Response struct {
	ProcessedCount int64 `json:"processedCount"`
	DurationMS     int64 `json:"durationMs"`
}

func Handler(ctx context.Context, req state.ImportInput) (resp Response, err error) {
	logger := log.Default.With(zap.String("sourceRegion", req.Source.Region),
		zap.String("sourceBucket", req.Source.Bucket),
		zap.String("sourceKey", req.Source.Key),
		zap.String("tableRegion", req.Target.Region),
		zap.String("tableName", req.Target.TableName),
		zap.Int64("sourceFromRange", req.Range[0]),
		zap.Int64("sourceToRange", req.Range[1]))
	logger.Info("starting", zap.Strings("numericFields", req.Source.NumericFields),
		zap.Strings("booleanFields", req.Source.BooleanFields),
		zap.Strings("cols", req.Columns),
		zap.String("delimiter", req.Source.Delimiter))

	start := time.Now()
	var duration time.Duration

	// Default to 8 concurrent Lambdas.
	if req.Configuration.LambdaConcurrency < 1 {
		req.Configuration.LambdaConcurrency = 8
	}
	if req.Source.Delimiter == "" {
		req.Source.Delimiter = ","
	}

	// Get the file from S3.
	src, err := get(req.Source.Region, req.Source.Bucket, req.Source.Key, req.Range[0], req.Range[1]-1)
	if err != nil {
		resp.DurationMS = time.Now().Sub(start).Milliseconds()
		return
	}

	// Parse the CSV data.
	csvr := csv.NewReader(src)
	csvr.Comma = rune(req.Source.Delimiter[0])
	conf := csvtodynamo.NewConfiguration()
	if req.Range[0] > 0 {
		csvr.FieldsPerRecord = len(req.Columns)
		conf.Columns = req.Columns
	}
	conf.AddNumberKeys(req.Source.NumericFields...)
	conf.AddBoolKeys(req.Source.BooleanFields...)
	reader, err := csvtodynamo.NewConverter(csvr, conf)
	if err != nil {
		logger.Error("failed to create CSV reader", zap.Error(err))
		return
	}
	bw, err := batchwriter.New(req.Target.Region, req.Target.TableName)
	if err != nil {
		logger.Error("failed to create batch writer", zap.Error(err))
		return
	}

	var recordCount int64

	// Start up workers.
	ctx, cancel := context.WithCancel(context.Background())
	batches := make(chan []map[string]*dynamodb.AttributeValue, 128) // 128 * 400KB max size allows the use of 50MB of RAM.
	var wg sync.WaitGroup
	wg.Add(req.Configuration.LambdaConcurrency)
	var errors []error
	for i := 0; i < req.Configuration.LambdaConcurrency; i++ {
		go func() {
			defer wg.Done()
			for batch := range batches {
				err := bw.Write(batch)
				if err != nil {
					logger.Error("error executing batch put", zap.Error(err))
					errors = append(errors, err)
					cancel()
					return
				}
				if recordCount := atomic.AddInt64(&recordCount, int64(len(batch))); recordCount%10000 == 0 {
					duration = time.Since(start)
					logger.Info("progress update",
						zap.Int64("records", recordCount),
						zap.Int("rps", int(float64(recordCount)/duration.Seconds())))
				}
				select {
				case <-ctx.Done():
					return
				default:
				}
			}
		}()
	}

	// Push data into the job queue.
fillJobQueue:
	for {
		batch, read, err := reader.ReadBatch()
		if err != nil && err != io.EOF {
			logger.Error("failed to read batch, closing down", zap.Error(err))
			cancel()
			wg.Wait()
			return resp, err
		}
		if read > 0 {
			select {
			case batches <- batch[:read]:
				break
			case <-ctx.Done():
				break fillJobQueue
			}
		}
		if err == io.EOF {
			break
		}
	}
	close(batches)

	// Wait for completion.
	wg.Wait()
	cancel()
	duration = time.Since(start)
	if len(errors) > 0 {
		logger.Error("batch execution failed", zap.Errors("errors", errors))
		err = errors[0]
		return
	}
	logger.Info("complete")

	resp.ProcessedCount = recordCount
	resp.DurationMS = time.Now().Sub(start).Milliseconds()
	return
}

func get(region, bucket, key string, from, to int64) (io.ReadCloser, error) {
	sess, err := session.NewSession(&aws.Config{
		Region: aws.String(region),
	})
	if err != nil {
		return nil, err
	}
	svc := s3.New(sess)
	goo, err := svc.GetObject(&s3.GetObjectInput{
		Bucket: &bucket,
		Key:    &key,
		Range:  aws.String(fmt.Sprintf("bytes=%d-%d", from, to)),
	})
	return goo.Body, err
}

func main() {
	lambda.Start(Handler)
}
