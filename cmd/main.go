package main

import (
	"encoding/csv"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/a-h/ddbimport/csvtodynamo"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

var regionFlag = flag.String("region", "", "The AWS region where the DynamoDB table is located")
var tableFlag = flag.String("table", "", "The DynamoDB table name to import to.")
var csvFlag = flag.String("csv", "", "The CSV file to upload to DynamoDB.")
var numericFieldsFlag = flag.String("numericFields", "", "A comma separated list of fields that are numeric.")
var booleanFieldsFlag = flag.String("booleanFields", "", "A comma separated list of fields that are boolean.")
var concurrencyFlag = flag.Int("concurrency", 4, "Number of imports to execute in parallel.")
var delimiterFlag = flag.String("delimiter", "comma", "The delimiter of the CSV file. Use the string 'tab' or 'comma'")

func delimiter(s string) rune {
	if s == "," || s == "\t" {
		return rune(s[0])
	}
	if s == "tab" {
		return '\t'
	}
	return ','
}

func main() {
	flag.Parse()
	if *regionFlag == "" || *tableFlag == "" || *csvFlag == "" {
		flag.PrintDefaults()
		os.Exit(1)
	}

	log.Printf("importing %q to %s in region %s", *csvFlag, *tableFlag, *regionFlag)

	start := time.Now()
	var duration time.Duration

	// Create dependencies.
	f, err := os.Open(*csvFlag)
	if err != nil {
		log.Fatalf("failed to open CSV file: %v", err)
	}
	defer f.Close()

	csvr := csv.NewReader(f)
	csvr.Comma = delimiter(*delimiterFlag)
	conf := csvtodynamo.NewConfiguration()
	conf.AddNumberKeys(strings.Split(*numericFieldsFlag, ",")...)
	conf.AddBoolKeys(strings.Split(*booleanFieldsFlag, ",")...)
	reader, err := csvtodynamo.NewConverter(csvr, conf)
	if err != nil {
		log.Fatalf("failed to create CSV reader: %v", err)
	}

	batchWriter, err := NewBatchWriter(*regionFlag, *tableFlag)
	if err != nil {
		log.Fatalf("failed to create batch writer: %v", err)
	}

	var batchCount int64 = 1
	var recordCount int64

	// Start up workers.
	batches := make(chan []map[string]*dynamodb.AttributeValue, 128) // 128 * 400KB max size allows the use of 50MB of RAM.
	var wg sync.WaitGroup
	wg.Add(*concurrencyFlag)
	for i := 0; i < *concurrencyFlag; i++ {
		go func() {
			defer wg.Done()
			for batch := range batches {
				err := batchWriter.Write(batch)
				if err != nil {
					log.Printf("error executing batch put: %v", err)
				}
				recordCount := atomic.AddInt64(&recordCount, int64(len(batch)))
				if batchCount := atomic.AddInt64(&batchCount, 1); batchCount%100 == 0 {
					duration = time.Since(start)
					log.Printf("%d records per second", int(float64(recordCount)/duration.Seconds()))
				}
			}
		}()
	}

	// Push data into the job queue.
	for {
		batch, _, err := reader.ReadBatch()
		if err != nil && err != io.EOF {
			log.Fatalf("failed to read batch %d: %v", batchCount, err)
		}
		batches <- batch
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
}

// NewBatchWriter creates a new BatchWriter to write to a DynamoDB table in batches.
func NewBatchWriter(region, tableName string) (bw BatchWriter, err error) {
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
