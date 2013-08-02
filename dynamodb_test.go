package dynamodb_test

import (
	"log"
	"testing"
	"time"

	"."
)

type Fetch struct {
	URL string `db:"HASH"`
}

var FETCH *Fetch

func TestCreateTablePutItemScanDeleteTable(t *testing.T) {
	d := &dynamodb.DynamoDB{} //d := &dynamodb.MemoryDB{}

	if err := d.CreateTableFor(FETCH); err != nil {
		t.Error(err)
	}

	for {
		if description, err := d.DescribeTableFor(FETCH); err != nil {
			t.Error(err)
		} else {
			log.Println(description.Table.TableStatus)
			if description.Table.TableStatus == "ACTIVE" {
				break
			}
		}
		time.Sleep(time.Second)
	}

	if err := d.Put(&Fetch{"http://localhost/"}); err != nil {
		t.Error(err)
	}

	if response, err := d.ScanFor(FETCH); err != nil {
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

	time.Sleep(60 * time.Second)

	if err := d.DeleteTableFor(FETCH); err != nil {
		t.Error(err)
	}
}
