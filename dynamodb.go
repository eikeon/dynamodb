// dynamodo...
package dynamodb

import (
	"errors"
	"fmt"
	"reflect"
	"strconv"
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
	tableType             reflect.Type
}

type TableDescription struct {
	Table struct {
		TableStatus string
	}
}

type Item map[string]map[string]string

type GetItemResponse struct {
	Item Item
}

type DeleteItem struct {
	TableName string
	Key       Key
}

type DeleteItemResponse struct {
	Attributes map[string]AttributeValue
}

type ScanResponse struct {
	Count        int
	ScannedCount int
	Items        []Item
}

type DynamoDB interface {
	Register(tableName string, i interface{}) (*Table, error)
	ToItem(s interface{}) Item
	ToKey(s interface{}) Key
	FromItem(string, Item) interface{}

	CreateTable(table *Table) error
	UpdateTable(tableName string, provisionedThroughput ProvisionedThroughput) error
	DescribeTable(tableName string) (*TableDescription, error)
	DeleteTable(tableName string) error
	PutItem(tableName string, item Item) error
	DeleteItem(deleteItem DeleteItem) (*DeleteItemResponse, error)
	GetItem(tableName string, key Key) (*GetItemResponse, error)
	Scan(tableName string) (*ScanResponse, error)
	Query(query *Query) (*QueryResponse, error)
}

type Tables map[string]*Table

func (tt Tables) Register(tableName string, i interface{}) (*Table, error) {
	tableType := reflect.TypeOf(i).Elem()

	t, err := tt.tableFor(tableName, tableType)
	if err != nil {
		return nil, err
	}

	tt[tableName] = t
	return t, nil
}

func (tt Tables) tableFor(tableName string, tableType reflect.Type) (*Table, error) {
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
	return &Table{tableName, keySchema, attributeDefinitions, provisionedThroughput, tableType}, nil
}

func (tt Tables) ToItem(s interface{}) Item {
	var it Item = make(map[string]map[string]string)
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

type AttributeValue map[string]string
type Key map[string]AttributeValue

type Condition struct {
	AttributeValueList []AttributeValue
	ComparisonOperator string
}

type KeyConditions map[string]Condition

type Query struct {
	TableName     string
	KeyConditions KeyConditions
}

type QueryResponse struct {
	Count int
	Items []Item
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
	et := tt[tableName].tableType
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
