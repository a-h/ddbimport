package csvtodynamo

import (
	"encoding/csv"

	"github.com/aws/aws-sdk-go/service/dynamodb"
)

// Converter converts CSV to DynamoDB records.
type Converter struct {
	r           *csv.Reader
	conf        *Configuration
	columnNames []string
}

type keyConverter func(s string) *dynamodb.AttributeValue

// NewConfiguration creates the Configuration for the Converter.
func NewConfiguration() *Configuration {
	return &Configuration{
		KeyToConverter: map[string]keyConverter{},
	}
}

// Configuration for the Converter.
type Configuration struct {
	KeyToConverter map[string]keyConverter
	Columns        []string
}

// AddStringKeys add string keys to the configuration.
func (conf *Configuration) AddStringKeys(s ...string) {
	for _, k := range s {
		conf.KeyToConverter[k] = stringValue
	}
}

// AddNumberKeys adds numeric keys to the configuration.
func (conf *Configuration) AddNumberKeys(s ...string) {
	for _, k := range s {
		conf.KeyToConverter[k] = numberValue
	}
}

// AddBoolKeys adds boolean keys to the configuration.
func (conf *Configuration) AddBoolKeys(s ...string) {
	for _, k := range s {
		conf.KeyToConverter[k] = boolValue
	}
}

func (c *Converter) init() error {
	if len(c.conf.Columns) > 0 {
		c.columnNames = c.conf.Columns
		return nil
	}
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
func (c *Converter) ReadBatch() (items []map[string]*dynamodb.AttributeValue, read int, err error) {
	batchSize := 25
	items = make([]map[string]*dynamodb.AttributeValue, batchSize)
	for read = 0; read < batchSize; read++ {
		items[read], err = c.Read()
		if err != nil {
			return items, read, err
		}
	}
	return items, read, err
}

// Read a single item from the CSV.
func (c *Converter) Read() (items map[string]*dynamodb.AttributeValue, err error) {
	record, err := c.r.Read()
	if err != nil {
		return
	}
	items = make(map[string]*dynamodb.AttributeValue, len(record))
	for i, column := range c.columnNames {
		if len(record[i]) != 0 {
			items[column] = c.dynamoValue(column, record[i])
		}
	}
	return items, err
}

// NewConverter creates a new CSV to DynamoDB converter.
func NewConverter(r *csv.Reader, conf *Configuration) (*Converter, error) {
	c := &Converter{
		r:    r,
		conf: conf,
	}
	err := c.init()
	return c, err
}

func (c *Converter) dynamoValue(key, value string) *dynamodb.AttributeValue {
	if f, ok := c.conf.KeyToConverter[key]; ok {
		return f(value)
	}
	return stringValue(value)
}

func stringValue(s string) *dynamodb.AttributeValue {
	return (&dynamodb.AttributeValue{}).SetS(s)
}

func numberValue(s string) *dynamodb.AttributeValue {
	return (&dynamodb.AttributeValue{}).SetN(s)
}

func boolValue(s string) *dynamodb.AttributeValue {
	if v, ok := boolValues[s]; ok {
		return v
	}
	return falseValue
}

var trueValue = (&dynamodb.AttributeValue{}).SetBOOL(true)
var falseValue = (&dynamodb.AttributeValue{}).SetBOOL(false)

var boolValues = map[string]*dynamodb.AttributeValue{
	"false": falseValue,
	"FALSE": falseValue,
	"true":  trueValue,
	"TRUE":  trueValue,
}
