package main

import (
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/a-h/ddbimport/batchwriter"
	"github.com/a-h/ddbimport/csvtodynamo"
	"github.com/a-h/ddbimport/sls/state"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/s3"
)

// Response from the Lambda.
type Response struct {
	ProcessedCount int64 `json:"processedCount"`
	DurationMS     int64 `json:"durationMs"`
}

func Handler(ctx context.Context, req state.ImportInput) (resp Response, err error) {
	start := time.Now()
	var duration time.Duration

	// Default to 8 concurrent Lambdas.
	if req.Configuration.LambdaConcurrency < 1 {
		req.Configuration.LambdaConcurrency = 8
	}
	if req.Source.Delimiter == "" {
		req.Source.Delimiter = ","
	}
	log.Printf("Processing bytes %v of S3 file (%s, %s, %s) with concurrency %d to DynamoDB table %s in region %s", req.Range, req.Source.Region, req.Source.Bucket, req.Source.Key, req.Configuration.LambdaConcurrency, req.Target.TableName, req.Target.Region)
	log.Printf("Using numeric fields: %v", req.Source.NumericFields)
	log.Printf("Using boolean fields: %v", req.Source.BooleanFields)
	log.Printf("Using delimiter: '%v'", req.Source.Delimiter)

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
		log.Fatalf("failed to create CSV reader: %v", err)
	}

	bw, err := batchwriter.New(req.Target.Region, req.Target.TableName)
	if err != nil {
		log.Printf("failed to create batch writer: %v", err)
		return
	}

	var batchCount int64 = 1
	var recordCount int64

	// Start up workers.
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
					log.Printf("error executing batch put: %v", err)
					errors = append(errors, err)
					return
				}
				recordCount := atomic.AddInt64(&recordCount, int64(len(batch)))
				if batchCount := atomic.AddInt64(&batchCount, 1); batchCount%500 == 0 {
					duration = time.Since(start)
					log.Printf("inserted %d batches (%d records) in %v - %d records per second", batchCount, recordCount, duration, int(float64(recordCount)/duration.Seconds()))
				}
			}
		}()
	}

	// Push data into the job queue.
	for {
		batch, read, err := reader.ReadBatch()
		if err != nil && err != io.EOF {
			log.Printf("failed to read batch: %v", err)
			return resp, err
		}
		if read > 0 {
			batches <- batch[:read]
		}
		if err == io.EOF {
			break
		}
	}
	close(batches)

	// Wait for completion.
	wg.Wait()
	duration = time.Since(start)
	if len(errors) > 0 {
		log.Printf("errors: %v", errors)
		err = errors[0]
		return
	}
	log.Printf("inserted %d batchCount (%d records) in %v - %d records per second", batchCount, recordCount, duration, int(float64(recordCount)/duration.Seconds()))
	log.Print("complete")

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
