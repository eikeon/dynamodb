// DynamoDB API Version 2012-08-10
package dynamodb

import (
	"errors"
	"fmt"
	"reflect"
	"strconv"
)

type AttributeDefinition struct {
	AttributeName string
	AttributeType string
}

type AttributeValue map[string]string

//AttributeValueUpdate

// +
type BatchGetItemOptions struct {
}

type BatchGetItemResult struct {
}

// +
type BatchWriteItemOptions struct {
}

type BatchWriteItemResult struct {
}

type Condition struct {
	AttributeValueList []AttributeValue
	ComparisonOperator string
}

//ConsumedCapacity

// +
type CreateTableOptions struct {
}

type CreateTableResult struct {
	TableDescription *TableDescription
}

// +
type DeleteItemOptions struct {
}

type DeleteItemResult struct {
	Attributes map[string]AttributeValue
}

// +
type DeleteTableOptions struct {
}

//DeleteRequest

type DeleteTableResult struct {
	TableDescription *TableDescription
}

// +
type DescribeTableOptions struct {
}

type DescribeTableResult struct {
	Table *TableDescription
}

//ExpectedAttributeValue

// +
type GetItemOptions struct {
}

type GetItemResult struct {
	Item Item
}

// +
type Item map[string]AttributeValue

//ItemCollectionMetrics

// +
type Key map[string]AttributeValue

// +
type KeyConditions map[string]Condition

type KeySchemaElement struct {
	AttributeName string
	KeyType       string
}

type KeysAndAttributes struct {
}

// +
type ListTablesOptions struct {
}

type ListTablesResult struct {
}

//LocalSecondaryIndex
//LocalSecondaryIndexDescription
//Projection

type ProvisionedThroughput struct {
	ReadCapacityUnits  int
	WriteCapacityUnits int
}

//ProvisionedThroughputDescription

// +
type PutItemOptions struct {
}

type PutItemResult struct {
}

//PutRequest

// +
type QueryOptions struct {
	KeyConditions KeyConditions
}

type QueryResult struct {
	Count int
	Items []Item
}

// +
type ScanOptions struct {
}

type ScanResult struct {
	Count        int
	ScannedCount int
	Items        []Item
}

type TableDescription struct {
	TableName             string
	KeySchema             []KeySchemaElement
	AttributeDefinitions  []AttributeDefinition
	ProvisionedThroughput ProvisionedThroughput
	TableStatus           string
}

// +
type UpdateItemOptions struct {
}

type UpdateItemResult struct {
}

// +
type UpdateTableOptions struct {
}

type UpdateTableResult struct {
}

type WriteRequest struct {
}

type actions interface {
	BatchGetItem(requestedItems map[string]KeysAndAttributes, options *BatchGetItemOptions) (*BatchGetItemResult, error)
	BatchWriteItem(requestedItems map[string]WriteRequest, options *BatchWriteItemOptions) (*BatchWriteItemResult, error)
	CreateTable(tableName string, attributeDefinitions []AttributeDefinition, keySchema []KeySchemaElement, ProvisionedThroughput ProvisionedThroughput, options *CreateTableOptions) (*CreateTableResult, error)
	DeleteItem(tableName string, key Key, options *DeleteItemOptions) (*DeleteItemResult, error)
	DeleteTable(tableName string, options *DeleteTableOptions) (*DeleteTableResult, error)
	DescribeTable(tableName string, options *DescribeTableOptions) (*DescribeTableResult, error)
	GetItem(tableName string, key Key, options *GetItemOptions) (*GetItemResult, error)
	ListTables(options *ListTablesOptions) (*ListTablesResult, error)
	PutItem(tableName string, item Item, options *PutItemOptions) (*PutItemResult, error)
	Query(tableName string, options *QueryOptions) (*QueryResult, error)
	Scan(tableName string, options *ScanOptions) (*ScanResult, error)
	UpdateItem(tableName string, key Key, options *UpdateItemOptions) (*UpdateItemResult, error)
	UpdateTable(tableName string, provisionedThroughput ProvisionedThroughput, options *UpdateTableOptions) (*UpdateTableResult, error)
}

type DynamoDB interface {
	actions

	Register(tableName string, i interface{}) (*TableDescription, error)
	ToItem(s interface{}) Item
	ToKey(s interface{}) Key
	FromItem(string, Item) interface{}
}

type Tables map[string]struct {
	TableDescription *TableDescription
	TableType        reflect.Type
}

func (tt Tables) Register(tableName string, i interface{}) (*TableDescription, error) {
	tableType := reflect.TypeOf(i).Elem()
	if t, err := tt.tableFor(tableName, tableType); err == nil {
		tt[tableName] = struct {
			TableDescription *TableDescription
			TableType        reflect.Type
		}{TableDescription: t, TableType: tableType}
		return t, nil
	} else {
		return nil, err
	}

}

func (tt Tables) tableFor(tableName string, tableType reflect.Type) (*TableDescription, error) {
	var primaryHash, primaryRange *KeySchemaElement
	var attributeDefinitions []AttributeDefinition
	var keySchema []KeySchemaElement
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
		if tag == "RANGE" {
			attributeDefinitions = append(attributeDefinitions, AttributeDefinition{name, attributeType})
			primaryRange = &KeySchemaElement{name, "RANGE"}
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
	return &TableDescription{TableName: tableName, KeySchema: keySchema, AttributeDefinitions: attributeDefinitions, ProvisionedThroughput: provisionedThroughput}, nil
}

func (tt Tables) ToItem(s interface{}) Item {
	var it Item = make(map[string]AttributeValue)
	sValue := reflect.ValueOf(s).Elem()
	typeOfItem := sValue.Type()

	for i := 0; i < sValue.NumField(); i++ {
		f := sValue.Field(i)
		name := typeOfItem.Field(i).Name
		switch f.Type().Kind() {
		case reflect.String:
			v := f.Interface().(string)
			if v != "" {
				it[name] = map[string]string{"S": v}
			}
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			s := strconv.FormatInt(f.Int(), 10)
			it[name] = map[string]string{"N": s}
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
			s := strconv.FormatUint(f.Uint(), 10)
			it[name] = map[string]string{"N": s}
		default:
			panic("attribute type not supported")
		}

	}
	return it
}

func (tt Tables) ToKey(s interface{}) Key {

	key := make(Key)

	sType := reflect.TypeOf(s).Elem()
	sValue := reflect.ValueOf(s).Elem()

	for i := 0; i < sValue.NumField(); i++ {
		sf := sType.Field(i)
		tag := sf.Tag.Get("db")
		if tag == "HASH" || tag == "RANGE" {
			fv := sValue.Field(i)
			switch sf.Type.Kind() {
			case reflect.String:
				key[sf.Name] = AttributeValue{"S": fv.Interface().(string)}
			case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
				key[sf.Name] = AttributeValue{"N": strconv.FormatInt(fv.Int(), 10)}
			default:
				panic("attribute type not supported")
			}
		}
	}
	return key
}

func (tt Tables) FromItem(tableName string, item Item) interface{} {
	et := tt[tableName].TableType
	v := reflect.New(et)
	v = v.Elem()
	switch v.Kind() {
	case reflect.Struct:
		for kk, vv := range item {
			if value, ok := vv["S"]; ok {
				f := v.FieldByName(kk)
				f.SetString(value)
			}
			if value, ok := vv["N"]; ok {
				f := v.FieldByName(kk)
				n, err := strconv.ParseInt(value, 10, 64)
				if err != nil || f.OverflowInt(n) {
					panic(fmt.Sprintf("%v %v\n", value, v.Type()))
				}
				f.SetInt(n)
			}
		}
	default:
		panic("Unsupported item type error")
	}
	return v.Addr().Interface()
}
