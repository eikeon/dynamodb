package dynamodb_test

import (
	"log"
	"testing"
	"time"

	"."
)

func TestCreateDeleteTable(t *testing.T) {
	ddb := &dynamodb.DynamoDB{}

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
	if err := ddb.DeleteTable("fetch"); err != nil {
		t.Error(err)
	}
}
