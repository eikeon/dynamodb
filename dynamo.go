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
	Tables
	client *aws4.Client
}

func NewDynamoDB() DynamoDB {
	d := &dynamo{Tables: make(Tables)}
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

func (db *dynamo) CreateTable(table *Table) error {
	reader, err := db.post("CreateTable", table)
	if err != nil {
		return err
	}
	if reader != nil {
		reader.Close()
	}
	return err
}

func (db *dynamo) UpdateTable(tableName string, provisionedThroughput ProvisionedThroughput) error {
	reader, err := db.post("UpdateTable", struct {
		TableName             string
		ProvisionedThroughput ProvisionedThroughput
	}{tableName, provisionedThroughput})
	if err != nil {
		return err
	}
	if reader != nil {
		reader.Close()
	}
	return err
}

func (db *dynamo) DescribeTable(tableName string) (*TableDescription, error) {
	reader, err := db.post("DescribeTable", struct {
		TableName string
	}{tableName})
	if err != nil {
		return nil, err
	}
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
	if err != nil {
		return err
	}
	if reader != nil {
		reader.Close()
	}
	return err
}

func (db *dynamo) PutItem(tableName string, item Item) error {
	reader, err := db.post("PutItem", struct {
		TableName string
		Item      Item
	}{tableName, item})
	if err != nil {
		return err
	}
	// TODO: decode response
	if reader != nil {
		reader.Close()
	}
	return err
}

func (db *dynamo) DeleteItem(deleteItem DeleteItem) (*DeleteItemResponse, error) {
	if reader, err := db.post("DeleteItem", deleteItem); err == nil {
		response := &DeleteItemResponse{}
		if err = json.NewDecoder(reader).Decode(&response); err != nil {
			return nil, err
		}
		reader.Close()
		return response, nil
	} else {
		return nil, err
	}
}

func (db *dynamo) GetItem(tableName string, key Key) (*GetItemResponse, error) {
	reader, err := db.post("GetItem", struct {
		TableName string
		Key       Key
	}{tableName, key})
	if err != nil {
		return nil, err
	}

	response := &GetItemResponse{}
	if err = json.NewDecoder(reader).Decode(&response); err != nil {
		return nil, err
	}
	if reader != nil {
		reader.Close()
	}

	return response, err
}

func (db *dynamo) Scan(tableName string) (*ScanResponse, error) {
	reader, err := db.post("Scan", struct {
		TableName string
	}{tableName})
	if err != nil {
		return nil, err
	}
	response := &ScanResponse{}
	if err = json.NewDecoder(reader).Decode(&response); err != nil {
		return nil, err
	}
	if reader != nil {
		reader.Close()
	}
	return response, nil
}

func (db *dynamo) Query(query *Query) (*QueryResponse, error) {
	reader, err := db.post("Query", query)
	if err != nil {
		return nil, err
	}
	response := &QueryResponse{}
	if err = json.NewDecoder(reader).Decode(&response); err != nil {
		return nil, err
	}
	if reader != nil {
		reader.Close()
	}
	return response, nil
}
