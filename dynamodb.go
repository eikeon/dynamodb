// dynamodo...
package dynamodb

import (
	"errors"
	"reflect"
)

type KeySchema []KeySchemaElement

type KeySchemaElement struct {
	AttributeName string
	KeyType       string
}

type AttributeDefinition struct {
	AttributeName string
	AttributeType string
}

type ProvisionedThroughput struct {
	ReadCapacityUnits  int
	WriteCapacityUnits int
}

type Table struct {
	TableName             string
	KeySchema             KeySchema
	AttributeDefinitions  []AttributeDefinition
	ProvisionedThroughput ProvisionedThroughput
}

type TableDescription struct {
	Table struct {
		TableStatus string
	}
}

type ScanResponse interface {
	GetCount() int
	GetScannedCount() int
	GetItems() []interface{}
}

type DynamoDB interface {
	Register(tableName string, i interface{})
	TableType(tableName string) reflect.Type
	CreateTable(tableName string) error
	UpdateTable(tableName string, provisionedThroughput ProvisionedThroughput) error
	DescribeTable(tableName string) (*TableDescription, error)
	DeleteTable(tableName string) error
	PutItem(tableName string, item interface{}) error
	GetItem(tableName string, key interface{}) (interface{}, error)
	Scan(tableName string) (ScanResponse, error)
}

func TableFor(tableName string, tableType reflect.Type) (*Table, error) {
	var primaryHash, primaryRange *KeySchemaElement
	var attributeDefinitions []AttributeDefinition
	var keySchema KeySchema
	provisionedThroughput := ProvisionedThroughput{1, 1}

	for i := 0; i < tableType.NumField(); i++ {
		f := tableType.Field(i)
		attributeType := ""
		switch f.Type.Kind() {
		case reflect.String:
			attributeType = "S"
		case reflect.Int, reflect.Int64:
			attributeType = "N"
		default:
			return nil, errors.New("attribute type not supported")
		}
		name := tableType.Field(i).Name

		tag := f.Tag.Get("db")
		if tag == "HASH" {
			attributeDefinitions = append(attributeDefinitions, AttributeDefinition{name, attributeType})
			primaryHash = &KeySchemaElement{name, "HASH"}
		}
	}

	if primaryHash == nil {
		return nil, errors.New("no primary key hash specified")
	} else {
		keySchema = append(keySchema, *primaryHash)
	}
	if primaryRange != nil {
		keySchema = append(keySchema, *primaryRange)
	}
	return &Table{tableName, keySchema, attributeDefinitions, provisionedThroughput}, nil
}
