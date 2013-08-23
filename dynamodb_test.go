package dynamodb_test

import (
	"fmt"
	"log"
	"net/url"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/eikeon/dynamodb"
)

var DB dynamodb.DynamoDB

var fetchrequestTableName string = "FetchRequest"

func init() {
	DB = dynamodb.NewDynamoDB()
	if hostname, err := os.Hostname(); err == nil {
		fetchrequestTableName = fetchrequestTableName + "-" + hostname
	} else {
		log.Println("error getting hostname:", err)
	}
}

type FetchRequest struct {
	Host        string `db:"HASH"`
	URLHash     string
	URL         string
	RequestedOn string `db:"RANGE"`
	RequestedBy string
}

func NewFetchRequest(URL string) (*FetchRequest, error) {
	if u, err := url.Parse(URL); err == nil {
		now := time.Now().Format(time.RFC3339Nano)
		return &FetchRequest{Host: u.Host, URL: URL, RequestedOn: now}, nil
	} else {
		return nil, err
	}
}

func testCreateTable(t *testing.T) {
	table, err := DB.Register(fetchrequestTableName, (*FetchRequest)(nil))
	if err != nil {
		t.Error(err)
	}
	table.ProvisionedThroughput.ReadCapacityUnits = 100
	table.ProvisionedThroughput.WriteCapacityUnits = 100
	if _, err := DB.CreateTable(table.TableName, table.AttributeDefinitions, table.KeySchema, table.ProvisionedThroughput, nil); err != nil {
		log.Println(err)
		//t.Error(err)
	}
}

func testDescribeTable(t *testing.T) {
	for {
		if description, err := DB.DescribeTable(fetchrequestTableName, nil); err != nil {
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

func putItem(j int) error {
	url := fmt.Sprintf("http://localhost-%d/%d", j, j)
	if f, err := NewFetchRequest(url); err == nil {
		if _, err := DB.PutItem(fetchrequestTableName, DB.ToItem(f), nil); err != nil {
			return err
		}
	} else {
		return err
	}
	return nil
}

func testGetItem(t *testing.T, j int) {
	url := fmt.Sprintf("http://localhost-%d/%d", j, j)
	if fr, err := NewFetchRequest(url); err == nil {
		log.Println("key:", DB.ToKey(fr))
		if f, err := DB.GetItem(fetchrequestTableName, DB.ToKey(fr), nil); err != nil {
			t.Error(err)
		} else {
			log.Println("f:", f)
			log.Println("Got:", DB.FromItem(fetchrequestTableName, f.Item))
		}
	} else {
		t.Error(err)
	}
}

func testScan(t *testing.T) {
	if response, err := DB.Scan(fetchrequestTableName, nil); err != nil {
		t.Error(err)
	} else {
		for i := 0; i < response.Count; i++ {
			item := DB.FromItem(fetchrequestTableName, response.Items[i])
			//if false { // TODO: vervose
			log.Println("item:", item)
			//}
		}
	}
}

func testDeleteTable(t *testing.T) {
	if _, err := DB.DeleteTable(fetchrequestTableName, nil); err != nil {
		t.Error(err)
	}

}

func TestAll(t *testing.T) {
	testCreateTable(t)
	testDescribeTable(t)

	if err := putItem(1); err != nil {
		t.Error(err)
	}
	testGetItem(t, 1)

	testScan(t)

	//testDeleteTable(t)
}

func benchmarkPutItem(b *testing.B, j int) {
	if err := putItem(j); err != nil {
		b.Error(err)
	}
}

func BenchmarkPutItem(b *testing.B) {
	for i := 0; i < b.N; i++ {
		benchmarkPutItem(b, i)
	}
}

func BenchmarkPutItemConcurrent(b *testing.B) {
	C := 16

	items := make(chan int, b.N)
	for i := 0; i < b.N; i++ {
		items <- i
	}
	close(items)

	var wg sync.WaitGroup
	wg.Add(C)
	for i := 0; i < C; i++ {
		go func() {
			for item := range items {
				benchmarkPutItem(b, item)
			}
			wg.Done()
		}()
	}
	wg.Wait()

}
