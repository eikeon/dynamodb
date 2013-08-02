// dynamodo...
package dynamodb

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"reflect"

	"github.com/eikeon/aws4"
)

func (db *DynamoDB) post(action string, parameters interface{}) (io.ReadCloser, error) {
	url := "https://dynamodb.us-east-1.amazonaws.com/"
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	if err := enc.Encode(parameters); err != nil {
		return nil, err
	}
	//log.Println(buf.String())
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
	CreateTableFor(s interface{})
	DescribeTableFor(s interface{})
	Put(item interface{}) error
	ScanFor(s interface{}) (*ScanResponse, error)
	DeleteTableFor(s interface{})
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

func (db *DynamoDB) CreateTableFor(i interface{}) error {
	var primaryHash, primaryRange *KeySchemaElement
	var attributeDefinitions []AttributeDefinition
	var keySchema KeySchema
	provisionedThroughput := ProvisionedThroughput{1, 1}

	s := reflect.ValueOf(i).Type().Elem()

	for i := 0; i < s.NumField(); i++ {
		f := s.Field(i)
		attributeType := ""
		switch f.Type.Kind() {
		case reflect.String:
			attributeType = "S"
		default:
			return errors.New("attribute type not supported")
		}
		name := s.Field(i).Name
		attributeDefinitions = append(attributeDefinitions, AttributeDefinition{name, attributeType})

		tag := f.Tag.Get("db")
		if tag == "HASH" {
			primaryHash = &KeySchemaElement{name, "HASH"}
		}
	}

	if primaryHash == nil {
		return errors.New("no primary key hash specified")
	} else {
		keySchema = append(keySchema, *primaryHash)
	}
	if primaryRange != nil {
		keySchema = append(keySchema, *primaryRange)
	}

	return db.CreateTable(s.Name(), attributeDefinitions, keySchema, provisionedThroughput)
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

func tableName(s interface{}) string {
	return reflect.ValueOf(s).Type().Elem().Name()
}

func (db *DynamoDB) DescribeTableFor(item interface{}) (*TableDescription, error) {
	return db.DescribeTable(tableName(item))
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

func (db *DynamoDB) DeleteTableFor(s interface{}) error {
	return db.DeleteTable(tableName(s))
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

type Item map[string]map[string]string

func (db *DynamoDB) Put(item interface{}) error {
	return db.PutItem(tableName(item), item)
}

func (db *DynamoDB) PutItem(tableName string, item interface{}) error {
	var it Item = make(map[string]map[string]string)
	s := reflect.ValueOf(item).Elem()
	typeOfItem := s.Type()

	for i := 0; i < s.NumField(); i++ {
		f := s.Field(i)
		name := typeOfItem.Field(i).Name
		switch f.Type().Kind() {
		case reflect.String:
			it[name] = map[string]string{"S": f.Interface().(string)}
		default:
			return errors.New("attribute type not supported")
		}

	}
	reader, err := db.post("PutItem", struct {
		TableName string
		Item      Item
	}{tableName, it})
	// TODO: decode response
	if reader != nil {
		reader.Close()
	}
	return err
}

type ScanResponse interface {
	Item(interface{}, int) error
	GetScannedCount() int
}

type dbScanResponse struct {
	Count        int64
	ScannedCount int
	Items        []Item
}

func (sr *dbScanResponse) GetScannedCount() int {
	return sr.ScannedCount
}

func (sr *dbScanResponse) Item(item interface{}, i int) (err error) {
	//log.Println(s, reflect.New(s))
	v := reflect.ValueOf(item)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}
	switch v.Kind() {
	case reflect.Struct:
		for kk, vv := range sr.Items[i] {
			if value, ok := vv["S"]; ok {
				f := v.FieldByName(kk)
				f.SetString(value)
			}
		}
	default:
		return errors.New("Unsupported item type error")
	}
	return
}

func (db *DynamoDB) ScanFor(s interface{}) (response ScanResponse, err error) {
	return db.Scan(tableName(s))
}

func (db *DynamoDB) Scan(tableName string) (response ScanResponse, err error) {
	reader, err := db.post("Scan", struct {
		TableName string
	}{tableName})
	response = &dbScanResponse{}
	if err = json.NewDecoder(reader).Decode(&response); err != nil {
		return nil, err
	}
	return
}
