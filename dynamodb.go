// dynamodo...
package dynamodb

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"io/ioutil"
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
	CreateTable(name string, attributeDefinitions []AttributeDefinition, keySchema KeySchema, provisionedThroughput ProvisionedThroughput)
	PutItem(tableName string, r interface{}) error
	Scan(tableName string) (*ScanResponse, error)
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

type Item map[string]map[string]string

func (db *DynamoDB) PutItem(tableName string, item interface{}) error {
	var it Item = make(map[string]map[string]string)
	v := reflect.ValueOf(item)
	switch v.Kind() {
	case reflect.Struct:
		num := v.NumField()
		t := v.Type()
		for i := 0; i < num; i++ {
			f := v.Field(i)
			switch f.Kind() {
			case reflect.String:
				it[t.Field(i).Name] = map[string]string{"S": f.String()}
			default:
				return errors.New("Unsupported field type error")
			}
		}
	default:
		return errors.New("Unsupported item type error")
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
