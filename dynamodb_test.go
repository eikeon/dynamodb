package dynamodb_test

import (
	"log"
	"testing"
	"time"

	"."
)

func TestCreateTablePutItemScanDeleteTable(t *testing.T) {
	ddb := &dynamodb.DynamoDB{}
	//ddb := &dynamodb.MemoryDB{}

	if err := ddb.CreateTable("fetch", []dynamodb.AttributeDefinition{{AttributeName: "URL", AttributeType: "S"}}, dynamodb.KeySchema{dynamodb.KeySchemaElement{"URL", "HASH"}}, dynamodb.ProvisionedThroughput{1, 1}); err != nil {
		t.Error(err)
	}

	for {
		if description, err := ddb.DescribeTable("fetch"); err != nil {
			t.Error(err)
		} else {
			log.Println(description.Table.TableStatus)
			if description.Table.TableStatus == "ACTIVE" {
				break
			}
		}
		time.Sleep(time.Second)
	}

	if err := ddb.PutItem("fetch", struct {
		URL string
	}{"http://localhost/"}); err != nil {
		t.Error(err)
	}

	if response, err := ddb.Scan("fetch"); err != nil {
		t.Error(err)
	} else {
		var item struct {
			URL string
		}
		for i := 0; i < response.GetScannedCount(); i++ {
			response.Item(&item, i)
			log.Println("item:", item)
		}
	}

	time.Sleep(5 * time.Second)

	//if err := ddb.DeleteTable("fetch"); err != nil {
	//	t.Error(err)
	//}
}
