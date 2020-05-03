package batchwriter

import (
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

// New creates a new BatchWriter to write to a DynamoDB table in batches.
func New(region, tableName string) (bw BatchWriter, err error) {
	sess, err := session.NewSession(&aws.Config{Region: aws.String(region)})
	if err != nil {
		return
	}
	bw = BatchWriter{
		client:    dynamodb.New(sess),
		tableName: tableName,
	}
	return
}

// BatchWriter writes to DynamoDB tables using BatchWriteItem.
type BatchWriter struct {
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
	_, err = bw.client.BatchWriteItem(&dynamodb.BatchWriteItemInput{
		RequestItems: map[string][]*dynamodb.WriteRequest{
			bw.tableName: writeRequests,
		},
	})
	if err != nil {
		err = fmt.Errorf("batchwriter: %w", err)
	}
	return
}
