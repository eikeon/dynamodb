package dynamodb_test

import (
	"fmt"
	"log"
	"testing"
	"time"

	"github.com/eikeon/dynamodb"
)

var DB dynamodb.DynamoDB

func init() {
	DB = dynamodb.NewMemoryDB()
}

type Fetch struct {
	URL         string `db:"HASH"`
	RequestedOn string `json:",omitempty"`
}

func testCreateTable(t *testing.T) {
	table, err := DB.Register("fetch", (*Fetch)(nil))
	if err != nil {
		t.Error(err)
	}

	if err := DB.CreateTable(table); err != nil {
		t.Error(err)
	}
}

func testDescribeTable(t *testing.T) {
	for {
		if description, err := DB.DescribeTable("fetch"); err != nil {
			t.Error(err)
		} else {
			log.Println(description.Table.TableStatus)
			if description.Table.TableStatus == "ACTIVE" {
				break
			}
		}
		time.Sleep(time.Second)
	}
}

func testPutItem(t *testing.T, j int) {
	url := fmt.Sprintf("http://localhost/%d", j)
	now := time.Now().Format(time.RFC3339Nano)
	f := &Fetch{url, now}
	if err := DB.PutItem("fetch", DB.ToItem(f)); err != nil {
		t.Error(err)
	}
}

func testGetItem(t *testing.T, j int) {
	url := fmt.Sprintf("http://localhost/%d", j)
	if f, err := DB.GetItem("fetch", DB.ToKey(&Fetch{URL: url})); err != nil {
		t.Error(err)
	} else {
		log.Println("Got:", DB.FromItem("fetch", f.Item))
	}
}

func testScan(t *testing.T) {
	if response, err := DB.Scan("fetch"); err != nil {
		t.Error(err)
	} else {
		for i := 0; i < response.Count; i++ {
			item := DB.FromItem("fetch", response.Items[i])
			log.Println("item:", item)
		}
	}
}

func testDeleteTable(t *testing.T) {
	if err := DB.DeleteTable("fetch"); err != nil {
		t.Error(err)
	}

}

func TestAll(t *testing.T) {
	testCreateTable(t)
	testDescribeTable(t)

	testPutItem(t, 1)
	testGetItem(t, 1)

	testScan(t)

	//testDeleteTable(t)
}

func benchmarkPutItem(b *testing.B, j int) {
	url := fmt.Sprintf("http://localhost/%d", j)
	now := time.Now().Format(time.RFC3339Nano)
	f := &Fetch{url, now}
	if err := DB.PutItem("fetch", DB.ToItem(f)); err != nil {
		b.Error(err)
	}
}

func BenchmarkPutItem(b *testing.B) {
	for i := 0; i < b.N; i++ {
		benchmarkPutItem(b, i)
	}
}
