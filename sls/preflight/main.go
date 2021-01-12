package main

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/a-h/ddbimport/log"
	"github.com/a-h/ddbimport/sls/preflight/process"
	"github.com/a-h/ddbimport/sls/state"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"go.uber.org/zap"
)

func Handler(ctx context.Context, req state.State) (resp state.State, err error) {
	logger := log.Default.With(zap.String("sourceRegion", req.Source.Region),
		zap.String("sourceBucket", req.Source.Bucket),
		zap.String("sourceKey", req.Source.Key),
		zap.String("tableRegion", req.Target.Region),
		zap.String("tableName", req.Target.TableName))
	logger.Info("starting", zap.Strings("numericFields", req.Source.NumericFields),
		zap.Strings("booleanFields", req.Source.BooleanFields),
		zap.String("delimiter", req.Source.Delimiter))

	if req.Source.Delimiter == "" {
		req.Source.Delimiter = ","
	}
	if req.Configuration.LambdaDurationSeconds < 30 {
		req.Configuration.LambdaDurationSeconds = 300
	}

	// Get the file from S3.
	src, srcSize, err := get(req.Source.Region, req.Source.Bucket, req.Source.Key, req.Preflight.Offset)
	if err != nil {
		return
	}
	defer src.Close()

	// Allocate records to workers in batches of 100,000 lines.
	// 100,000 lines / 25 BatchWriteOperations = 4000 operations per allocation.
	// At 3000 records per second, each batch is 30 seconds of work.
	var workerBatch int64 = 100000

	start := time.Now()
	hasTimedOut := func() bool {
		return time.Since(start) > req.Configuration.LambdaDurationSeconds*time.Second
	}
	return process.Process(logger, hasTimedOut, src, srcSize, workerBatch, req)
}

func get(region, bucket, key string, startIndex int64) (io.ReadCloser, int64, error) {
	sess, err := session.NewSession(&aws.Config{
		Region: aws.String(region),
	})
	if err != nil {
		return nil, -1, err
	}
	svc := s3.New(sess)
	goo, err := svc.GetObject(&s3.GetObjectInput{
		Bucket: &bucket,
		Key:    &key,
		Range:  aws.String(fmt.Sprintf("%d-", startIndex)),
	})
	return goo.Body, *goo.ContentLength, err
}

func main() {
	lambda.Start(Handler)
}
