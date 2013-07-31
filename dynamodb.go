// dynamodo...
package dynamodb

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"io/ioutil"
	"net/http"

	"github.com/eikeon/aws4"
)

func (db *DynamoDB) post(action string, parameters interface{}) (io.ReadCloser, error) {
	url := "https://dynamodb.us-east-1.amazonaws.com/"
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	if err := enc.Encode(parameters); err != nil {
		return nil, err
	}
	if request, err := http.NewRequest("POST", url, &buf); err != nil {
		return nil, err
	} else {
		request.Header.Set("Content-Type", "application/x-amz-json-1.0")
		request.Header.Set("X-Amz-Target", "DynamoDB_20120810"+"."+action)

		if response, err := db.getClient().Do(request); err == nil {
			if response.StatusCode == 200 {
				return response.Body, nil
			} else {
				b, err := ioutil.ReadAll(response.Body)
				if err != nil {
					return nil, err
				}
				return nil, errors.New(string(b))
			}
		} else {
			return nil, err
		}
	}
}

type KeySchema []KeySchemaElement

type Table struct {
	TableName            string
	KeySchema            KeySchema
	AttributeDefinitions []AttributeDefinition
}

type KeySchemaElement struct {
	AttributeName string
	KeyType       string
}

type AttributeDefinition struct {
	AttributeName string
	AttributeType string
}

type DB interface {
	CreateTable(name string, attributeDefinitions []AttributeDefinition, keySchema KeySchema)
	PutItem(tableName string, r interface{}) error
	Scan(tableName string) ([]interface{}, error)
}

type DynamoDB struct {
	client *aws4.Client
}

func (b *DynamoDB) getClient() *aws4.Client {
	if b.client == nil {
		b.client = aws4.DefaultClient
	}
	return b.client
}

type ProvisionedThroughput struct {
	ReadCapacityUnits  int
	WriteCapacityUnits int
}

func (db *DynamoDB) CreateTable(tableName string, attributeDefinitions []AttributeDefinition, keySchema []KeySchemaElement, provisionedThroughput ProvisionedThroughput) error {
	reader, err := db.post("CreateTable", struct {
		TableName             string
		AttributeDefinitions  []AttributeDefinition
		KeySchema             []KeySchemaElement
		ProvisionedThroughput ProvisionedThroughput
	}{tableName, attributeDefinitions, keySchema, provisionedThroughput})
	if reader != nil {
		reader.Close()
	}
	return err
}

type TableDescription struct {
	Table struct {
		TableStatus string
	}
}

func (db *DynamoDB) DescribeTable(tableName string) (*TableDescription, error) {
	reader, err := db.post("DescribeTable", struct {
		TableName string
	}{tableName})
	var description TableDescription
	if err = json.NewDecoder(reader).Decode(&description); err != nil {
		return nil, err
	}
	if reader != nil {
		reader.Close()
	}
	return &description, err
}

func (db *DynamoDB) DeleteTable(tableName string) error {
	reader, err := db.post("DeleteTable", struct {
		TableName string
	}{tableName})
	if reader != nil {
		reader.Close()
	}
	return err
}

func (b *DynamoDB) PutItem(tableName string, item interface{}) error {
	panic("NYI")
}

func (b *DynamoDB) Scan(tableName string) (items []interface{}, err error) {
	panic("NYI")
}
