package main

import (
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/a-h/ddbimport/batchwriter"
	"github.com/a-h/ddbimport/csvtodynamo"
	"github.com/a-h/ddbimport/log"
	"github.com/a-h/ddbimport/sls/state"
	_ "github.com/a-h/ddbimport/sls/statik"
	"github.com/a-h/ddbimport/version"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/cloudformation"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/sfn"
	"github.com/google/uuid"
	"github.com/rakyll/statik/fs"
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
var stepFnRegionFlag = flag.String("stepFnRegion", "", "The AWS region of the ddbimport Step Function.")
var installFlag = flag.Bool("install", false, "Set to install the ddbimport Step Function.")
var remoteFlag = flag.Bool("remote", false, "Set when the import should be carried out using the ddbimport Step Function.")

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

func printUsageAndExit(suffix ...string) {
	fmt.Println("usage: ddbimport [<args>]")
	fmt.Println("version:", version.Version)
	fmt.Println()
	fmt.Println("Import local CSV from this computer:")
	fmt.Println("  ddbimport -inputFile ../data.csv -delimiter tab -numericFields year -tableRegion eu-west-2 -tableName ddbimport")
	fmt.Println()
	fmt.Println("Import S3 file from this computer:")
	fmt.Println("  ddbimport -bucketRegion eu-west-2 -bucketName infinityworks-ddbimport -bucketKey data1M.csv -delimiter tab -numericFields year -tableRegion eu-west-2 -tableName ddbimport")
	fmt.Println()
	fmt.Println("Import S3 file using remote ddbimport Step Function:")
	fmt.Println("  ddbimport -remote -bucketRegion eu-west-2 -bucketName infinityworks-ddbimport -bucketKey data1M.csv -delimiter tab -numericFields year -tableRegion eu-west-2 -tableName ddbimport")
	fmt.Println()
	fmt.Println("Install ddbimport Step Function:")
	fmt.Println("  ddbimport -install -stepFnRegion=eu-west-2")
	fmt.Println()
	flag.Usage()
	for _, s := range suffix {
		fmt.Println(s)
	}
	os.Exit(1)
}

func main() {
	flag.Parse()
	if *installFlag {
		if *stepFnRegionFlag == "" {
			printUsageAndExit("Must pass stepFnRegion")
		}
		install(*stepFnRegionFlag)
		return
	}
	if *tableRegionFlag == "" || *tableNameFlag == "" {
		printUsageAndExit("Must include a table region and table name flag.")
	}
	numericFields := strings.Split(*numericFieldsFlag, ",")
	booleanFields := strings.Split(*booleanFieldsFlag, ",")
	localFile := *inputFileFlag != ""
	remoteFile := *bucketRegionFlag != "" || *bucketNameFlag != "" || *bucketKeyFlag != ""
	if localFile && remoteFile {
		printUsageAndExit("Must pass inputFile OR bucketRegion, bucketName and bucketKey.")
	}
	if remoteFile && (*bucketRegionFlag == "" || *bucketNameFlag == "" || *bucketKeyFlag == "") {
		printUsageAndExit("Must pass values for all of the bucketRegion, bucketName and bucketKey arguments if a localFile argument is omitted.")
	}
	if *remoteFlag {
		if !remoteFile {
			printUsageAndExit("Remote import requires the file to be located within an S3 bucket. Pass the bucketRegion, bucketName and bucketKey arguments.")
		}
		stepFnRegion := *tableRegionFlag
		if *stepFnRegionFlag != "" {
			stepFnRegion = *stepFnRegionFlag
		}
		input := state.Input{
			Source: state.Source{
				Region:        *bucketRegionFlag,
				Bucket:        *bucketNameFlag,
				Key:           *bucketKeyFlag,
				NumericFields: numericFields,
				BooleanFields: booleanFields,
				Delimiter:     string(delimiter(*delimiterFlag)),
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
		return
	}

	// Import local.
	inputName := *inputFileFlag
	input := func() (io.ReadCloser, error) { return os.Open(*inputFileFlag) }
	if remoteFile {
		inputName = fmt.Sprintf("s3://%s/%s (%s)", url.PathEscape(*bucketNameFlag), url.PathEscape(*bucketKeyFlag), *bucketRegionFlag)
		input = func() (io.ReadCloser, error) { return s3Get(*bucketRegionFlag, *bucketNameFlag, *bucketKeyFlag) }
	}
	importLocal(input, inputName, numericFields, booleanFields, delimiter(*delimiterFlag), *tableRegionFlag, *tableNameFlag, *concurrencyFlag)
}

func setLambdaFunctionS3Location(template map[string]interface{}, zipLocation string) {
	changeKey(template, zipLocation, "Resources", "PreflightLambdaFunction", "Properties", "Code", "S3Key")
	changeKey(template, zipLocation, "Resources", "ImportLambdaFunction", "Properties", "Code", "S3Key")
	return
}

// changeKey within JSON document.
func changeKey(node map[string]interface{}, newValue string, path ...string) {
	if len(path) == 0 {
		return
	}
	key := path[0]
	if value, ok := node[key]; ok {
		if len(path) == 1 {
			node[key] = newValue
			return
		}
		if child, ok := value.(map[string]interface{}); ok {
			changeKey(child, newValue, path[1:]...)
		}
	}
}

// getServerlessPackageFile e.g. /ddbimport.zip
func getServerlessPackageFile(path string) (file http.File, err error) {
	statikFS, err := fs.New()
	if err != nil {
		return
	}
	return statikFS.Open(path)
}

func getStackID(c *cloudformation.CloudFormation, name string) (stackID *string, err error) {
	err = c.ListStacksPages(&cloudformation.ListStacksInput{}, func(lso *cloudformation.ListStacksOutput, lastPage bool) bool {
		for _, s := range lso.StackSummaries {
			if *s.StackName == name {
				stackID = s.StackId
				return false
			}
		}
		return true
	})
	return
}

func s3Put(region, bucket, key string, data io.ReadSeeker) error {
	sess, err := session.NewSession(&aws.Config{
		Region: aws.String(region),
	})
	if err != nil {
		return err
	}
	svc := s3.New(sess)
	_, err = svc.PutObject(&s3.PutObjectInput{
		Bucket: &bucket,
		Key:    &key,
		Body:   data,
	})
	return err
}

func install(region string) {
	log.Default.Info("installing ddbimport Step Function")
	sess, err := session.NewSession(&aws.Config{Region: aws.String(region)})
	if err != nil {
		log.Default.Fatal("failed to create AWS session", zap.Error(err))
	}
	c := cloudformation.New(sess)

	// Check to see if the stack already exists.
	stackID, err := getStackID(c, "ddbimport")
	if err != nil {
		log.Default.Fatal("failed to list stacks", zap.Error(err))
	}
	if stackID == nil {
		// Deploy it if it doesn't exist.
		log.Default.Info("creating ddbimport stack")
		f, err := getServerlessPackageFile("/cloudformation-template-create-stack.json")
		if err != nil {
			log.Default.Fatal("failed to get creation CloudFormation template", zap.Error(err))
			return
		}
		defer f.Close()
		createStackTemplate, err := ioutil.ReadAll(f)
		if err != nil {
			log.Default.Fatal("failed to read create CloudFormation template", zap.Error(err))
		}
		cso, err := c.CreateStack(&cloudformation.CreateStackInput{
			Capabilities: aws.StringSlice([]string{cloudformation.CapabilityCapabilityIam, cloudformation.CapabilityCapabilityNamedIam}),
			StackName:    aws.String("ddbimport"),
			TemplateBody: aws.String(string(createStackTemplate)),
		})
		if err != nil {
			log.Default.Fatal("failed to create stack", zap.Error(err))
		}
		stackID = cso.StackId
		log.Default.Info("created stack", zap.String("stackId", *cso.StackId))
		return
	}

	// Deploy the zip to S3.
	// Find the ServerlessDeploymentBucket.
	log.Default.Info("uploading Lambda zip")
	var s3Bucket string
	dso, err := c.DescribeStacks(&cloudformation.DescribeStacksInput{
		StackName: stackID,
	})
	if err != nil {
		log.Default.Fatal("failed to describe the stack", zap.Error(err))
	}
	if len(dso.Stacks) == 0 {
		log.Default.Fatal("failed to find the stack")
	}
	for _, o := range dso.Stacks[0].Outputs {
		if *o.OutputKey == "ServerlessDeploymentBucketName" {
			s3Bucket = *o.OutputValue
			break
		}
	}
	if s3Bucket == "" {
		log.Default.Fatal("could not find S3 bucket")
	}
	// Upload the zip.
	lambdaZip, err := getServerlessPackageFile("/ddbimport.zip")
	if err != nil {
		log.Default.Fatal("failed to get the Lambda zip")
	}
	s3Path := version.Version + "/ddbimport.zip"
	err = s3Put(region, s3Bucket, s3Path, lambdaZip)
	if err != nil {
		log.Default.Fatal("failed to upload the Lambda zip", zap.Error(err))
	}
	log.Default.Info("zip upload complete")

	// Update the CloudFormation stack to set the Lambda location.
	log.Default.Info("updating ddbimport stack")
	// Get the update file.
	f, err := getServerlessPackageFile("/cloudformation-template-update-stack.json")
	if err != nil {
		log.Default.Fatal("failed to get update CloudFormation template", zap.Error(err))
	}
	defer f.Close()
	// Decode it and update the S3 location based on the current version.
	d := json.NewDecoder(f)
	var updateStackTemplate map[string]interface{}
	err = d.Decode(&updateStackTemplate)
	if err != nil {
		log.Default.Fatal("failed to decode update CloudFormation template", zap.Error(err))
	}
	setLambdaFunctionS3Location(updateStackTemplate, s3Path)
	updateStackTemplateJSON, err := json.Marshal(updateStackTemplate)
	if err != nil {
		log.Default.Fatal("failed to encode updated update CloudFormation template", zap.Error(err))
	}
	// Execute the update.
	_, err = c.UpdateStack(&cloudformation.UpdateStackInput{
		Capabilities: aws.StringSlice([]string{cloudformation.CapabilityCapabilityIam, cloudformation.CapabilityCapabilityNamedIam}),
		StackName:    stackID,
		TemplateBody: aws.String(string(updateStackTemplateJSON)),
	})
	if err != nil {
		log.Default.Fatal("failed to update stack", zap.Error(err))
	}
	log.Default.Info("ddbimport step function succesfully deployed")
}

func importRemote(stepFnRegion string, input state.Input) {
	logger := log.Default.With(zap.String("sourceRegion", input.Source.Region),
		zap.String("sourceBucket", input.Source.Bucket),
		zap.String("sourceKey", input.Source.Key),
		zap.String("delimiter", input.Source.Delimiter),
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

func s3Get(region, bucket, key string) (io.ReadCloser, error) {
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

func importLocal(input func() (io.ReadCloser, error), inputName string, numericFields, booleanFields []string, delimiter rune, tableRegion, tableName string, concurrency int) {
	logger := log.Default.With(zap.String("input", inputName),
		zap.String("tableRegion", tableRegion),
		zap.String("tableName", tableName))

	logger.Info("starting local import")

	start := time.Now()
	var duration time.Duration

	// Create dependencies.
	f, err := input()
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
		if len(batch) > 0 {
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
	logger.Info("complete",
		zap.Int64("records", recordCount),
		zap.Int("rps", int(float64(recordCount)/duration.Seconds())),
		zap.Duration("duration", duration))
}
