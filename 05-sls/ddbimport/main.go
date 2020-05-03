package main

import (
	"context"
	"encoding/csv"
	"io"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/a-h/ddbimport/batchwriter"
	"github.com/a-h/ddbimport/csvtodynamo"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/s3"
)

// Request to the ddbimport Lambda.
type Request struct {
	Source Source `json:"source"`
	Worker Worker `json:"worker"`
	Target Target `json:"target"`
}

// Source of the CSV data to import.
type Source struct {
	Region        string   `json:"region"`
	Bucket        string   `json:"bucket"`
	Key           string   `json:"key"`
	NumericFields []string `json:"numericFields"`
	BooleanFields []string `json:"booleanFields"`
	Delimiter     string   `json:"delimiter"`
}

// Target DynamoDB table.
type Target struct {
	Region    string `json:"region"`
	TableName string `json:"tableName"`
}

// Worker details.
type Worker struct {
	// Index of the worker in the set, e.g. 0, 1, 2, 3 out of a 4 worker set.
	Index int `json:"index"`
	// Count of total workers, e.g. 4.
	Count int `json:"count"`
	// Concurrency of each worker. Defaults to 1.
	Concurrency int `json:"concurrency"`
}

// Response from the Lambda.
type Response struct {
	Worker         Worker `json:"worker"`
	Error          error  `json:"error"`
	ProcessedCount int64  `json:"processedCount"`
	DurationMS     int64  `json:"durationMs"`
}

func Handler(ctx context.Context, req Request) (resp Response, err error) {
	resp.Worker = req.Worker
	start := time.Now()
	var duration time.Duration

	// Optionally be able to divide work up across workers.
	if req.Worker.Count == 0 {
		req.Worker.Count = 1
		req.Worker.Index = 0
	}
	if req.Worker.Concurrency < 1 {
		req.Worker.Concurrency = 1
	}
	if req.Source.Delimiter == "" {
		req.Source.Delimiter = ","
	}
	log.Printf("Worker index %d out of %d processing S3 file (%s, %s, %s) with concurrency %d to DynamoDB table %s in region %s", req.Worker.Index, req.Worker.Count, req.Source.Region, req.Source.Bucket, req.Source.Key, req.Worker.Concurrency, req.Target.TableName, req.Target.Region)
	log.Printf("Using numeric fields: %v", req.Source.NumericFields)
	log.Printf("Using boolean fields: %v", req.Source.BooleanFields)
	log.Printf("Using delimiter: '%v'", req.Source.Delimiter)

	// Get the file from S3.
	src, err := get(req.Source.Region, req.Source.Bucket, req.Source.Key)
	if err != nil {
		resp.DurationMS = time.Now().Sub(start).Milliseconds()
		resp.Error = err
		return
	}

	// Parse the CSV data.
	csvr := csv.NewReader(src)
	csvr.Comma = rune(req.Source.Delimiter[0])
	conf := csvtodynamo.NewConfiguration()
	conf.AddNumberKeys(req.Source.NumericFields...)
	conf.AddBoolKeys(req.Source.BooleanFields...)
	reader, err := csvtodynamo.NewConverter(csvr, conf)
	if err != nil {
		log.Fatalf("failed to create CSV reader: %v", err)
	}

	bw, err := batchwriter.New(req.Target.Region, req.Target.TableName)
	if err != nil {
		log.Fatalf("failed to create batch writer: %v", err)
	}

	var batchCount int64 = 1
	var recordCount int64

	// Start up workers.
	batches := make(chan []map[string]*dynamodb.AttributeValue, 128) // 128 * 400KB max size allows the use of 50MB of RAM.
	var wg sync.WaitGroup
	wg.Add(req.Worker.Concurrency)
	for i := 0; i < req.Worker.Concurrency; i++ {
		go func() {
			defer wg.Done()
			for batch := range batches {
				err := bw.Write(batch)
				if err != nil {
					log.Printf("error executing batch put: %v", err)
				}
				recordCount := atomic.AddInt64(&recordCount, int64(len(batch)))
				if batchCount := atomic.AddInt64(&batchCount, 1); batchCount%100 == 0 {
					duration = time.Since(start)
					log.Printf("inserted %d batches (%d records) in %v - %d records per second", batchCount, recordCount, duration, int(float64(recordCount)/duration.Seconds()))
				}
			}
		}()
	}

	// Push data into the job queue.
	batchIndex := 0
	for {
		batch, _, err := reader.ReadBatch()
		if err != nil && err != io.EOF {
			log.Fatalf("failed to read batch %d: %v", batchIndex, err)
		}
		batchIndex++
		// Only process the batch if it's designated for this worker.
		assignedWorkerIndex := batchIndex % req.Worker.Count
		if assignedWorkerIndex == req.Worker.Index {
			batches <- batch
		}
		if err == io.EOF {
			break
		}
	}
	close(batches)

	// Wait for completion.
	wg.Wait()
	duration = time.Since(start)
	log.Printf("inserted %d batchCount (%d records) in %v - %d records per second", batchCount, recordCount, duration, int(float64(recordCount)/duration.Seconds()))
	log.Print("complete")

	resp.ProcessedCount = recordCount
	resp.DurationMS = time.Now().Sub(start).Milliseconds()
	return
}

func get(region, bucket, key string) (io.ReadCloser, error) {
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
	})
	return goo.Body, err
}

func main() {
	lambda.Start(Handler)
}
