package main

import (
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/a-h/ddbimport/batchwriter"
	"github.com/a-h/ddbimport/csvtodynamo"
	"github.com/a-h/ddbimport/log"
	"github.com/a-h/ddbimport/sls/state"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/sfn"
	"github.com/google/uuid"
	"go.uber.org/zap"
)

// Target DynamoDB table.
var tableRegionFlag = flag.String("tableRegion", "", "The AWS region where the DynamoDB table is located")
var tableNameFlag = flag.String("tableName", "", "The DynamoDB table name to import to.")

// Source bucket.
var bucketRegionFlag = flag.String("bucketRegion", "", "The AWS region where the source bucket is located")
var bucketNameFlag = flag.String("bucketName", "", "The name of the S3 bucket containing the data file.")
var bucketKeyFlag = flag.String("bucketKey", "", "The file within the S3 bucket that contains the data.")

// Local configuration.
var inputFileFlag = flag.String("inputFile", "", "The local CSV file to upload to DynamoDB. You must pass the csv flag OR the key and bucket flags.")

// Remote configuration.
var stepFnRegionFlag = flag.String("stepFnRegion", "", "The AWS region where the step function has been installed. If left blank, the DynamoDB region is used.")

// Global configuration.
var numericFieldsFlag = flag.String("numericFields", "", "A comma separated list of fields that are numeric.")
var booleanFieldsFlag = flag.String("booleanFields", "", "A comma separated list of fields that are boolean.")
var delimiterFlag = flag.String("delimiter", "comma", "The delimiter of the CSV file. Use the string 'tab' or 'comma'")
var concurrencyFlag = flag.Int("concurrency", 8, "Number of imports to execute in parallel.")

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
	if *tableRegionFlag == "" || *tableNameFlag == "" {
		flag.Usage()
		os.Exit(1)
	}
	numericFields := strings.Split(*numericFieldsFlag, ",")
	booleanFields := strings.Split(*booleanFieldsFlag, ",")
	if *inputFileFlag != "" {
		if *bucketRegionFlag != "" || *bucketNameFlag != "" || *bucketKeyFlag != "" {
			fmt.Println("Must not pass the bucketRegion, bucketName and bucketKey arguments if inputFile is set.")
			flag.Usage()
			os.Exit(1)
		}
		// Import local.
		importLocal(*inputFileFlag, numericFields, booleanFields, delimiter(*delimiterFlag), *tableRegionFlag, *tableNameFlag, *concurrencyFlag)
	}
	if *bucketRegionFlag == "" || *bucketNameFlag == "" || *bucketKeyFlag == "" {
		fmt.Println("Must pass the bucketRegion, bucketName and bucketKey arguments if inputFile is omitted.")
		flag.Usage()
		os.Exit(1)
	}

	// Import remote.
	stepFnRegion := *tableRegionFlag
	if *stepFnRegionFlag != "" {
		stepFnRegion = *stepFnRegionFlag
	}
	input := state.Input{
		Source: state.Source{
			Region:        *bucketRegionFlag,
			Bucket:        *bucketNameFlag,
			Key:           *bucketKeyFlag,
			NumericFields: strings.Split(*numericFieldsFlag, ","),
			BooleanFields: strings.Split(*booleanFieldsFlag, ","),
			Delimiter:     *delimiterFlag,
		},
		Configuration: state.Configuration{
			LambdaConcurrency:     *concurrencyFlag,
			LambdaDurationSeconds: 900,
		},
		Target: state.Target{
			Region:    *tableRegionFlag,
			TableName: *tableNameFlag,
		},
	}
	importRemote(stepFnRegion, input)
}

func importRemote(stepFnRegion string, input state.Input) {
	logger := log.Default.With(zap.String("sourceRegion", input.Source.Region),
		zap.String("sourceBucket", input.Source.Bucket),
		zap.String("sourceKey", input.Source.Key),
		zap.String("tableRegion", input.Target.Region),
		zap.String("tableName", input.Target.TableName))

	logger.Info("starting import")

	sess, err := session.NewSession(&aws.Config{Region: aws.String(stepFnRegion)})
	if err != nil {
		logger.Fatal("failed to create AWS session", zap.Error(err))
	}
	c := sfn.New(sess)

	// Find the ARN of the ddbimport state machine.
	var arn *string
	err = c.ListStateMachinesPages(&sfn.ListStateMachinesInput{
		MaxResults: aws.Int64(1000),
	}, func(lsmo *sfn.ListStateMachinesOutput, lastPage bool) bool {
		for _, sm := range lsmo.StateMachines {
			if *sm.Name == "ddbimport" {
				arn = sm.StateMachineArn
				return false
			}
		}
		return true
	})
	if err != nil {
		logger.Fatal("failed to list state machines", zap.Error(err))
	}
	if arn == nil {
		logger.Fatal("ddbimport state machine not found. Have you deployed the ddbimport Step Function?")
	}
	logger = logger.With(zap.String("stepFunctionArn", *arn))
	logger.Info("found ARN")

	executionID := uuid.New().String()
	payload, err := json.Marshal(input)
	if err != nil {
		logger.Fatal("failed to marshal input", zap.Error(err))
	}

	seo, err := c.StartExecution(&sfn.StartExecutionInput{
		Input:           aws.String(string(payload)),
		Name:            aws.String(executionID),
		StateMachineArn: arn,
	})
	if err != nil {
		logger.Fatal("failed to start execution of state machine", zap.Error(err))
	}
	executionArn := seo.ExecutionArn
	logger = logger.With(zap.String("executionArn", *executionArn))
	logger.Info("started execution")

	var outputPayload string
waitForOutput:
	for {
		deo, err := c.DescribeExecution(&sfn.DescribeExecutionInput{
			ExecutionArn: executionArn,
		})
		if err != nil {
			logger.Fatal("failed to get execution status", zap.Error(err))
		}
		switch *deo.Status {
		case sfn.ExecutionStatusRunning:
			logger.Info("execution running")
			time.Sleep(time.Second * 5)
			continue
		case sfn.ExecutionStatusSucceeded:
			logger.Info("execution succeeded")
			outputPayload = *deo.Output
			break waitForOutput
		default:
			logger.Fatal("unexpected execution status", zap.String("status", *deo.Status))
		}
	}

	var output []sfnResponse
	err = json.Unmarshal([]byte(outputPayload), &output)
	if err != nil {
		logger.Fatal("failed to unmarshal output", zap.String("output", outputPayload), zap.Error(err))
	}
	var lines int64
	for _, op := range output {
		lines += op.ProcessedCount
	}
	logger.Info("complete", zap.Int64("lines", lines))
}

type sfnResponse struct {
	ProcessedCount int64 `json:"processedCount"`
	DurationMS     int64 `json:"durationMs"`
}

func importLocal(inputFile string, numericFields, booleanFields []string, delimiter rune, tableRegion, tableName string, concurrency int) {
	logger := log.Default.With(zap.String("inputFile", inputFile),
		zap.String("tableRegion", tableRegion),
		zap.String("tableName", tableName))

	logger.Info("starting local import")

	start := time.Now()
	var duration time.Duration

	// Create dependencies.
	f, err := os.Open(inputFile)
	if err != nil {
		logger.Fatal("failed to open input file", zap.Error(err))
	}
	defer f.Close()

	csvr := csv.NewReader(f)
	csvr.Comma = delimiter
	conf := csvtodynamo.NewConfiguration()
	conf.AddNumberKeys(numericFields...)
	conf.AddBoolKeys(booleanFields...)
	reader, err := csvtodynamo.NewConverter(csvr, conf)
	if err != nil {
		logger.Fatal("failed to create CSV reader", zap.Error(err))
	}

	batchWriter, err := batchwriter.New(tableRegion, tableName)
	if err != nil {
		logger.Fatal("failed to create batch writer", zap.Error(err))
	}

	var batchCount int64 = 1
	var recordCount int64

	// Start up workers.
	batches := make(chan []map[string]*dynamodb.AttributeValue, 128) // 128 * 400KB max size allows the use of 50MB of RAM.
	var wg sync.WaitGroup
	wg.Add(concurrency)
	for i := 0; i < concurrency; i++ {
		go func(workerIndex int) {
			defer wg.Done()
			for batch := range batches {
				err := batchWriter.Write(batch)
				if err != nil {
					logger.Error("error executing batch write", zap.Int("workerIndex", workerIndex), zap.Error(err))
					return
				}
				recordCount := atomic.AddInt64(&recordCount, int64(len(batch)))
				if batchCount := atomic.AddInt64(&batchCount, 1); batchCount%100 == 0 {
					duration = time.Since(start)
					logger.Info("progress", zap.Int("workerIndex", workerIndex), zap.Int64("records", recordCount), zap.Int("rps", int(float64(recordCount)/duration.Seconds())))
				}
			}
		}(i)
	}

	// Push data into the job queue.
	for {
		batch, _, err := reader.ReadBatch()
		if err != nil && err != io.EOF {
			logger.Fatal("failed to read batch from input",
				zap.Int64("batchCount", batchCount),
				zap.Error(err))
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
	logger.Info("complete",
		zap.Int64("records", recordCount),
		zap.Int("rps", int(float64(recordCount)/duration.Seconds())),
		zap.Duration("duration", duration))
}
