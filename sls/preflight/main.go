package main

import (
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"time"

	"github.com/a-h/ddbimport/sls/linereader"
	"github.com/a-h/ddbimport/sls/state"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

func Handler(ctx context.Context, req state.State) (resp state.State, err error) {
	start := time.Now()
	if req.Source.Delimiter == "" {
		req.Source.Delimiter = ","
	}
	if req.Configuration.LambdaDurationSeconds < 30 {
		req.Configuration.LambdaDurationSeconds = 300
	}
	log.Printf("Preflight check of S3 file (%s, %s, %s) with Lambda concurrency %d to DynamoDB table %s in region %s", req.Source.Region, req.Source.Bucket, req.Source.Key, req.Configuration.LambdaConcurrency, req.Target.TableName, req.Target.Region)
	log.Printf("Using numeric fields: %v", req.Source.NumericFields)
	log.Printf("Using boolean fields: %v", req.Source.BooleanFields)
	log.Printf("Using delimiter: '%v'", req.Source.Delimiter)

	// Get the file from S3.
	src, err := get(req.Source.Region, req.Source.Bucket, req.Source.Key, req.Preflight.Offset)
	if err != nil {
		return
	}
	defer src.Close()

	resp = req
	maxDuration := req.Configuration.LambdaDurationSeconds * time.Second

	// Allocate records to workers in batches of 100,000 lines.
	// 100,000 lines / 25 BatchWriteOperations = 4000 operations per allocation.
	// At 3000 records per second, each batch is 30 seconds of work.
	var workerBatch int64 = 100000

	// Parse the CSV data, keeping track of the byte position in the file.
	lines := resp.Preflight.Line
	batchStartIndex := req.Preflight.Offset
	lr := linereader.New(src, resp.Preflight.Line, resp.Preflight.Offset, func(line, offset int64) {
		lines++
		resp.Preflight.Line = line
		resp.Preflight.Offset = offset
		if lines%workerBatch == 0 {
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
			continue
		}
		if time.Since(start) > maxDuration {
			timedOut = true
			break
		}
		if err == io.EOF {
			err = nil
			break
		}
		recordCount++
		if recordCount%50000 == 0 {
			log.Printf("Processed %d records", recordCount)
		}
	}
	log.Printf("Processed %d lines in %v", lines, time.Since(start))
	if resp.Preflight.Offset != lr.Offset {
		resp.Batches = append(resp.Batches, []int64{batchStartIndex, lr.Offset})
		resp.Preflight.Offset = lr.Offset
	}
	if timedOut {
		log.Printf("Timed out, starting from current position in new Lambda...")
		return
	}
	log.Print("Complete. Exiting...")
	return
}

func get(region, bucket, key string, startIndex int64) (io.ReadCloser, error) {
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
		Range:  aws.String(fmt.Sprintf("%d-", startIndex)),
	})
	return goo.Body, err
}

func main() {
	lambda.Start(Handler)
}
