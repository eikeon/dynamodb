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

type dynamo struct {
	client    *aws4.Client
	tableType map[string]reflect.Type
}

func NewDynamoDB() DynamoDB {
	return &dynamo{}
}

func (b *dynamo) getClient() *aws4.Client {
	if b.client == nil {
		b.client = aws4.DefaultClient
	}
	return b.client
}

func (db *dynamo) post(action string, parameters interface{}) (io.ReadCloser, error) {
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

func (db *dynamo) Register(tableName string, i interface{}) {
	tableType := reflect.TypeOf(i).Elem()
	if db.tableType == nil {
		db.tableType = make(map[string]reflect.Type)
	}
	db.tableType[tableName] = tableType
}

func (db *dynamo) TableType(tableName string) reflect.Type {
	return db.tableType[tableName]
}

func (db *dynamo) CreateTable(tableName string) error {
	t, err := TableFor(tableName, db.TableType(tableName))
	if err != nil {
		return err
	}
	reader, err := db.post("CreateTable", t)
	if reader != nil {
		reader.Close()
	}
	return err
}

func (db *dynamo) DescribeTable(tableName string) (*TableDescription, error) {
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

func (db *dynamo) DeleteTable(tableName string) error {
	reader, err := db.post("DeleteTable", struct {
		TableName string
	}{tableName})
	if reader != nil {
		reader.Close()
	}
	return err
}

func (db *dynamo) PutItem(tableName string, item interface{}) error {
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

type Item map[string]map[string]string

type dbScanResponse struct {
	Count        int
	ScannedCount int
	Items        []Item
	items        []interface{}
}

func (sr *dbScanResponse) GetCount() int {
	return sr.Count
}

func (sr *dbScanResponse) GetScannedCount() int {
	return sr.ScannedCount
}

func (sr *dbScanResponse) GetItems() []interface{} {
	return sr.items
}

func (db *dynamo) Scan(tableName string) (ScanResponse, error) {
	et := db.tableType[tableName]
	reader, err := db.post("Scan", struct {
		TableName string
	}{tableName})
	response := &dbScanResponse{}
	if err = json.NewDecoder(reader).Decode(&response); err != nil {
		return nil, err
	}
	for i := 0; i < response.Count; i++ {
		v := reflect.New(et)
		v = v.Elem()
		switch v.Kind() {
		case reflect.Struct:
			for kk, vv := range response.Items[i] {
				if value, ok := vv["S"]; ok {
					f := v.FieldByName(kk)
					f.SetString(value)
				}
			}
		default:
			return nil, errors.New("Unsupported item type error")
		}
		response.items = append(response.items, v.Interface())
	}
	return response, nil
}
