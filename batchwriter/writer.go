package batchwriter

import (
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

// New creates a new BatchWriter to write to a DynamoDB table in batches.
// It uses the default Backoff implementation which provides up to 7 retries
// costing 25 seconds of latency before failing the entire batch.
func New(region, tableName string) (bw BatchWriter, err error) {
	sess, err := session.NewSession(&aws.Config{Region: aws.String(region)})
	if err != nil {
		return
	}
	bw = BatchWriter{
		Backoff:   NewBackoff(7),
		client:    dynamodb.New(sess),
		tableName: tableName,
	}
	return
}

// BatchWriter writes to DynamoDB tables using BatchWriteItem.
type BatchWriter struct {
	Backoff   Backoff
	client    *dynamodb.DynamoDB
	tableName string
}

// Write to DynamoDB using BatchWriteItem.
func (bw BatchWriter) Write(records []map[string]*dynamodb.AttributeValue) (err error) {
	writeRequests := make([]*dynamodb.WriteRequest, len(records))
	for i := 0; i < len(records); i++ {
		writeRequests[i] = &dynamodb.WriteRequest{
			PutRequest: &dynamodb.PutRequest{
				Item: records[i],
			},
		}
	}
	requestItems := map[string][]*dynamodb.WriteRequest{
		bw.tableName: writeRequests,
	}
	return bw.write(requestItems, 0)
}

func (bw BatchWriter) write(ri map[string][]*dynamodb.WriteRequest, retry int) (err error) {
	bwo, err := bw.client.BatchWriteItem(&dynamodb.BatchWriteItemInput{
		RequestItems: ri,
	})
	if err != nil {
		err = fmt.Errorf("batchwriter: %w", err)
		return
	}
	if len(bwo.UnprocessedItems) > 0 {
		if err = bw.Backoff(retry); err != nil {
			return err
		}
		return bw.write(bwo.UnprocessedItems, retry+1)
	}
	return
}

// Backoff function to retry during batch writes.
type Backoff func(retry int) error

// ErrMaxBackoffReached is returned when the maximum backoff count has been reached.
var ErrMaxBackoffReached = errors.New("backoff: max backoff reached")

// NewBackoff creates a backoff function.
// Retry Backoff Total elapsed
// 0     0       0
// 1     200     0.2
// 2     400     0.6
// 3     800     1.4
// 4     1600    3
// 5     3200    6.2
// 6     6400    12.6
// 7     12800   25.4
func NewBackoff(maxRetries int) Backoff {
	return func(retry int) error {
		if retry > maxRetries {
			return ErrMaxBackoffReached
		}
		if retry == 0 {
			return nil
		}
		t := math.Pow(2.0, float64(retry))
		sleepFor := time.Duration(t) * (100 * time.Millisecond)
		time.Sleep(sleepFor)
		return nil
	}
}
