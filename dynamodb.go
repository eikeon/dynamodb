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
	Register(tableName string, i interface{}) (*Table, error)
	CreateTable(table *Table) error
	UpdateTable(tableName string, provisionedThroughput ProvisionedThroughput) error
	DescribeTable(tableName string) (*TableDescription, error)
	DeleteTable(tableName string) error
	PutItem(tableName string, item interface{}) error
	GetItem(tableName string, key interface{}) (interface{}, error)
	Scan(tableName string) (ScanResponse, error)
}

type TableType map[string]reflect.Type

func (tt TableType) Register(tableName string, i interface{}) (*Table, error) {
	tableType := reflect.TypeOf(i).Elem()
	tt[tableName] = tableType

	t, err := tt.tableFor(tableName, tableType)
	if err != nil {
		return nil, err
	}
	return t, nil
}

func (tt TableType) tableFor(tableName string, tableType reflect.Type) (*Table, error) {
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
