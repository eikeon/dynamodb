package driver

import (
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

type Driver interface {
	Register(tableName string, tableType reflect.Type)
	TableType(tableName string) reflect.Type
	CreateTable(tableName string) error
	DescribeTable(tableName string) (*TableDescription, error)
	DeleteTable(tableName string) error
	PutItem(tableName string, item interface{}) error
	Scan(tableName string) (ScanResponse, error)
}
