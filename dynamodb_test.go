package dynamodb_test

import (
	"log"
	"testing"
	"time"

	"github.com/eikeon/dynamodb"
)

type Fetch struct {
	URL         string `db:"HASH"`
	RequestedOn string `json:",omitempty"`
}

func TestCreateTablePutItemScanDeleteTable(t *testing.T) {
	for _, d := range []dynamodb.DynamoDB{dynamodb.NewMemoryDB(), dynamodb.NewDynamoDB()} {
		d.Register("fetch", (*Fetch)(nil))

		if err := d.CreateTable("fetch"); err != nil {
			t.Error(err)
		}

		for {
			if description, err := d.DescribeTable("fetch"); err != nil {
				t.Error(err)
			} else {
				log.Println(description.Table.TableStatus)
				if description.Table.TableStatus == "ACTIVE" {
					break
				}
			}
			time.Sleep(time.Second)
		}

		now := time.Now().Format(time.RFC3339Nano)
		f := &Fetch{"http://localhost/", now}
		if err := d.PutItem("fetch", f); err != nil {
			t.Error(err)
		}

		time.Sleep(time.Second)

		if f, err := d.GetItem("fetch", &Fetch{URL: "http://localhost/"}); err != nil {
			t.Error(err)
		} else {
			log.Println("Got:", f)
		}

		if response, err := d.Scan("fetch"); err != nil {
			t.Error(err)
		} else {
			items := response.GetItems()
			for i := 0; i < response.GetCount(); i++ {
				item := items[i]
				log.Println("item:", item)
			}
		}

		time.Sleep(10 * time.Second)

		if err := d.DeleteTable("fetch"); err != nil {
			t.Error(err)
		}
	}
}
