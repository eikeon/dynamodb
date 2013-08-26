package dynamodb

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math"
	"net/http"
	"time"

	"github.com/eikeon/aws4"
)

type dynamo struct {
	mapping
	client *aws4.Client
}

func NewDynamoDB() DynamoDB {
	d := &dynamo{mapping: make(mapping)}
	if d.getClient() == nil {
		log.Println("could not create dynamodb: no default aws4 client")
		return nil
	}
	return d
}

func (b *dynamo) getClient() *aws4.Client {
	if b.client == nil {
		b.client = aws4.DefaultClient
		if b.client != nil {
			tr := &http.Transport{DisableKeepAlives: false, MaxIdleConnsPerHost: 100}
			c := &http.Client{Transport: tr}
			b.client.Client = c
		}
	}
	return b.client
}

func (db *dynamo) post(action string, parameters interface{}) (io.ReadCloser, error) {
	url := "https://dynamodb.us-east-1.amazonaws.com/"
	currentRetry := 0
	maxNumberOfRetries := 10
RETRY:
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
			switch response.StatusCode {
			case 200:
				return response.Body, nil
			case 400:
				type ResponseError struct {
					StatusCode int
					Type       string `json:"__type"`
					Message    string
				}
				var error ResponseError
				if err = json.NewDecoder(response.Body).Decode(&error); err != nil {
					return nil, err
				}
				response.Body.Close()
				if error.Type == "com.amazonaws.dynamodb.v20120810#ProvisionedThroughputExceededException" {
					log.Println("Provisioned throughput exceeded... retrying:", action)
				} else {
					return nil, errors.New(fmt.Sprintf("%#v", error))
				}
			case 500:
				response.Body.Close()
				log.Println("Got a 500 error... retrying.")
			default:
				b, err := ioutil.ReadAll(response.Body)
				response.Body.Close()
				if err != nil {
					return nil, err
				}
				return nil, errors.New(string(b))
			}
		} else {
			return nil, err
		}
		if currentRetry < maxNumberOfRetries {
			wait := time.Duration(math.Pow(2, float64(currentRetry))) * 50 * time.Millisecond
			time.Sleep(wait)
			currentRetry = currentRetry + 1
			goto RETRY
		} else {
			return nil, errors.New("exceeded maximum number of retries")
		}

	}
}

func (db *dynamo) BatchGetItem(requestItems map[string]KeysAndAttributes, options *BatchGetItemOptions) (*BatchGetItemResult, error) {
	if reader, err := db.post("BatchGetItem", struct {
		RequestItems map[string]KeysAndAttributes
		*BatchGetItemOptions
	}{requestItems, options}); err == nil {
		response := &BatchGetItemResult{}
		if err = json.NewDecoder(reader).Decode(&response); err != nil {
			return nil, err
		}
		reader.Close()
		return response, nil
	} else {
		return nil, err
	}
}

func (db *dynamo) BatchWriteItem(requestItems map[string]WriteRequest, options *BatchWriteItemOptions) (*BatchWriteItemResult, error) {
	if reader, err := db.post("BatchGetItem", struct {
		RequestItems map[string]WriteRequest
		*BatchWriteItemOptions
	}{requestItems, options}); err == nil {
		response := &BatchWriteItemResult{}
		if err = json.NewDecoder(reader).Decode(&response); err != nil {
			return nil, err
		}
		reader.Close()
		return response, nil
	} else {
		return nil, err
	}
}

func (db *dynamo) CreateTable(tableName string, attributeDefinitions []AttributeDefinition, keySchema []KeySchemaElement, provisionedThroughput ProvisionedThroughput, options *CreateTableOptions) (*CreateTableResult, error) {
	table := struct {
		TableName             string
		AttributeDefinitions  []AttributeDefinition
		KeySchema             []KeySchemaElement
		ProvisionedThroughput ProvisionedThroughput
		*CreateTableOptions
	}{TableName: tableName, AttributeDefinitions: attributeDefinitions, KeySchema: keySchema, ProvisionedThroughput: provisionedThroughput, CreateTableOptions: options}
	reader, err := db.post("CreateTable", table)
	if err != nil {
		return nil, err
	}
	var result CreateTableResult
	if err = json.NewDecoder(reader).Decode(&result); err != nil {
		return nil, err
	}
	if reader != nil {
		reader.Close()
	}
	return &result, nil
}

func (db *dynamo) UpdateItem(tableName string, key Key, options *UpdateItemOptions) (*UpdateItemResult, error) {
	if reader, err := db.post("BatchGetItem", struct {
		TableName string
		Key       Key
		*UpdateItemOptions
	}{tableName, key, options}); err == nil {
		response := &UpdateItemResult{}
		if err = json.NewDecoder(reader).Decode(&response); err != nil {
			return nil, err
		}
		reader.Close()
		return response, nil
	} else {
		return nil, err
	}
}

func (db *dynamo) UpdateTable(tableName string, provisionedThroughput ProvisionedThroughput, options *UpdateTableOptions) (*UpdateTableResult, error) {
	if reader, err := db.post("UpdateTable", struct {
		TableName             string
		ProvisionedThroughput ProvisionedThroughput
		*UpdateTableOptions
	}{tableName, provisionedThroughput, options}); err == nil {
		response := &UpdateTableResult{}
		if err = json.NewDecoder(reader).Decode(&response); err != nil {
			return nil, err
		}
		reader.Close()
		return response, nil
	} else {
		return nil, err
	}
}

func (db *dynamo) DescribeTable(tableName string, options *DescribeTableOptions) (*DescribeTableResult, error) {
	reader, err := db.post("DescribeTable", struct {
		TableName string
		*DescribeTableOptions
	}{tableName, options})
	if err != nil {
		return nil, err
	}
	var description DescribeTableResult
	if err = json.NewDecoder(reader).Decode(&description); err != nil {
		return nil, err
	}
	if reader != nil {
		reader.Close()
	}
	return &description, err
}

func (db *dynamo) DeleteTable(tableName string, options *DeleteTableOptions) (*DeleteTableResult, error) {
	if reader, err := db.post("DeleteTable", struct {
		TableName string
		*DeleteTableOptions
	}{tableName, options}); err == nil {
		response := &DeleteTableResult{}
		if err = json.NewDecoder(reader).Decode(&response); err != nil {
			return nil, err
		}
		reader.Close()
		return response, nil
	} else {
		return nil, err
	}
}

func (db *dynamo) PutItem(tableName string, item Item, options *PutItemOptions) (*PutItemResult, error) {
	if reader, err := db.post("PutItem", struct {
		TableName string
		Item      Item
		*PutItemOptions
	}{tableName, item, options}); err == nil {
		response := &PutItemResult{}
		if err = json.NewDecoder(reader).Decode(&response); err != nil {
			return nil, err
		}
		reader.Close()
		return response, nil
	} else {
		return nil, err
	}
}

func (db *dynamo) DeleteItem(tableName string, key Key, options *DeleteItemOptions) (*DeleteItemResult, error) {
	if reader, err := db.post("DeleteItem", struct {
		TableName string
		Key       Key
		*DeleteItemOptions
	}{TableName: tableName, Key: key, DeleteItemOptions: options}); err == nil {
		response := &DeleteItemResult{}
		if err = json.NewDecoder(reader).Decode(&response); err != nil {
			return nil, err
		}
		reader.Close()
		return response, nil
	} else {
		return nil, err
	}
}

func (db *dynamo) GetItem(tableName string, key Key, options *GetItemOptions) (*GetItemResult, error) {
	reader, err := db.post("GetItem", struct {
		TableName string
		Key       Key
		*GetItemOptions
	}{tableName, key, options})
	if err != nil {
		return nil, err
	}

	response := GetItemResult{}
	if err = json.NewDecoder(reader).Decode(&response); err != nil {
		return nil, err
	}
	if reader != nil {
		reader.Close()
	}

	return &response, err
}

func (db *dynamo) ListTables(options *ListTablesOptions) (*ListTablesResult, error) {
	return nil, errors.New("NYI")
}

func (db *dynamo) Scan(tableName string, options *ScanOptions) (*ScanResult, error) {
	reader, err := db.post("Scan", struct {
		TableName string
		*ScanOptions
	}{tableName, options})
	if err != nil {
		return nil, err
	}
	response := &ScanResult{}
	if err = json.NewDecoder(reader).Decode(&response); err != nil {
		return nil, err
	}
	if reader != nil {
		reader.Close()
	}
	return response, nil
}

func (db *dynamo) Query(tableName string, options *QueryOptions) (*QueryResult, error) {
	query := struct {
		TableName string
		*QueryOptions
	}{TableName: tableName, QueryOptions: options}
	reader, err := db.post("Query", query)
	if err != nil {
		return nil, err
	}
	response := &QueryResult{}
	if err = json.NewDecoder(reader).Decode(&response); err != nil {
		return nil, err
	}
	if reader != nil {
		reader.Close()
	}
	return response, nil
}
