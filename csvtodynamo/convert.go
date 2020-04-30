package csvtodynamo

import (
	"encoding/csv"

	"github.com/aws/aws-sdk-go/service/dynamodb"
)

// Converter converts CSV to DynamoDB records.
type Converter struct {
	r           *csv.Reader
	columnNames []string
}

func (c *Converter) init() error {
	record, err := c.r.Read()
	if err != nil {
		return err
	}
	if c.columnNames == nil {
		c.columnNames = record
	}
	return nil
}

// ReadBatch reads 25 items from the CSV.
// Only strings, numbers and boolean values are supported in CSV.
func (c *Converter) ReadBatch(items []map[string]*dynamodb.AttributeValue) (itms []map[string]*dynamodb.AttributeValue, read int, err error) {
	batchSize := 25
	if items == nil {
		items = make([]map[string]*dynamodb.AttributeValue, batchSize)
	}
	for read = 0; read < batchSize; read++ {
		items[read], err = c.Read(items[read])
		if err != nil {
			return items, read, err
		}
	}
	return items, read, err
}

// Read a single item from the CSV.
func (c *Converter) Read(items map[string]*dynamodb.AttributeValue) (itms map[string]*dynamodb.AttributeValue, err error) {
	record, err := c.r.Read()
	if err != nil {
		return items, err
	}
	if items == nil {
		items = make(map[string]*dynamodb.AttributeValue, len(record))
	}
	for i, column := range c.columnNames {
		if len(record[i]) != 0 {
			items[column] = dynamoValue(record[i])
		}
	}
	return items, err
}

// NewConverter creates a new CSV to DynamoDB converter.
func NewConverter(r *csv.Reader) (*Converter, error) {
	c := &Converter{
		r: r,
	}
	err := c.init()
	return c, err
}

func dynamoValue(s string) *dynamodb.AttributeValue {
	if v, ok := boolValues[s]; ok {
		return v
	}
	if isNumeric(s) {
		return (&dynamodb.AttributeValue{}).SetN(s)
	}
	return (&dynamodb.AttributeValue{}).SetS(s)
}

var numericRunes = map[rune]struct{}{
	'0': {},
	'1': {},
	'2': {},
	'3': {},
	'4': {},
	'5': {},
	'6': {},
	'7': {},
	'8': {},
	'9': {},
}

func isNumeric(s string) bool {
	var periods int
	for i, r := range s {
		// Allow leading negative.
		if i == 0 && r == '-' {
			continue
		}
		// Allow a single UK/US format period.
		// If you want European style conversion (1.234,56 vs 1,234.56).
		if r == '.' {
			periods++
			if periods > 1 {
				return false
			}
			continue
		}
		if _, ok := numericRunes[r]; !ok {
			return false
		}
	}
	return true
}

var trueValue = (&dynamodb.AttributeValue{}).SetBOOL(true)
var falseValue = (&dynamodb.AttributeValue{}).SetBOOL(false)

var boolValues = map[string]*dynamodb.AttributeValue{
	"false": falseValue,
	"FALSE": falseValue,
	"true":  trueValue,
	"TRUE":  trueValue,
}
