package state

import "time"

// Input to the ddbimport step function.
type Input struct {
	Source        Source        `json:"src"`
	Configuration Configuration `json:"cnf"`
	Target        Target        `json:"tgt"`
}

// State of the ddbimport Step Function. Source and Target must be populated.
// The Workers field is populated by the preflight Lambda in the Step Function.
type State struct {
	Input
	Preflight Preflight `json:"prefl"`
	// Batches of ranges (from, to).
	Batches [][]int64 `json:"batches"`
}

// ImportInput is the input to the ddbimport.
type ImportInput struct {
	Input
	// Range of bytes.
	Range   []int64  `json:"range"`
	Columns []string `json:"cols"`
}

// Source of the CSV data to import.
type Source struct {
	Region        string   `json:"region"`
	Bucket        string   `json:"bucket"`
	Key           string   `json:"key"`
	NumericFields []string `json:"numFlds"`
	BooleanFields []string `json:"boolFlds"`
	Delimiter     string   `json:"delim"`
}

// Configuration of the Step Function.
type Configuration struct {
	// LambdaConcurrency is the number of BatchWriteItem requests that will be executed in parallel.
	LambdaConcurrency int `json:"lambdaConcur"`
	// LambdaDurationSeconds is the minimum amount of time each Lambda will spend executing tasks.
	// After exceeding this, the preflight will start again.
	LambdaDurationSeconds time.Duration `json:"lambdaDurSecs"`
}

// Target DynamoDB table.
type Target struct {
	Region    string `json:"region"`
	TableName string `json:"table"`
}

// Preflight reads through the file to determine how many lines there are in the file, and to
// divide up the work into chunks for the wokers.
type Preflight struct {
	// Line is the current line within the file.
	Line int64 `json:"l"`
	// Offset is the byte offset within the file.
	Offset int64 `json:"o"`
	// Continue to carry on working.
	Continue bool `json:"cnt"`

	// Columns is the set of columns in the file.
	Columns []string `json:"cols"`
}
