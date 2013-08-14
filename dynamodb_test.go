package dynamodb_test

import (
	"fmt"
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
		table, err := d.Register("fetch", (*Fetch)(nil))
		if err != nil {
			t.Error(err)
		}

		if err := d.CreateTable(table); err != nil {
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

		for j := 0; j < 100; j++ {
			url := fmt.Sprintf("http://localhost/%d", j)
			now := time.Now().Format(time.RFC3339Nano)
			f := &Fetch{url, now}
			if err := d.PutItem("fetch", d.ToItem(f)); err != nil {
				t.Error(err)
			}
		}

		time.Sleep(time.Second)

		for j := 0; j < 100; j++ {
			url := fmt.Sprintf("http://localhost/%d", j)
			if f, err := d.GetItem("fetch", d.ToKey(&Fetch{URL: url})); err != nil {
				t.Error(err)
			} else {
				log.Println("Got:", d.FromItem("fetch", f.Item))
			}
		}

		if response, err := d.Scan("fetch"); err != nil {
			t.Error(err)
		} else {
			for i := 0; i < response.Count; i++ {
				item := d.FromItem("fetch", response.Items[i])
				log.Println("item:", item)
			}
		}

		time.Sleep(10 * time.Second)

		if err := d.DeleteTable("fetch"); err != nil {
			t.Error(err)
		}
	}
}
