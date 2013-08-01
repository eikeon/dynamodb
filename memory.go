package dynamodb

import (
	"errors"
	"reflect"
)

type table struct {
	definition *Table
	items      map[string]interface{}
}

type MemoryDB struct {
	tables map[string]*table
}

func (b *MemoryDB) CreateTable(name string, attributeDefinitions []AttributeDefinition, keySchema KeySchema, provisionedThroughput ProvisionedThroughput) error {
	definition := Table{name, keySchema, attributeDefinitions}
	if b.tables == nil {
		b.tables = make(map[string]*table)
	}
	b.tables[name] = &table{definition: &definition, items: make(map[string]interface{})}
	return nil
}

func (db *MemoryDB) DescribeTable(tableName string) (*TableDescription, error) {
	td := TableDescription{}
	td.Table.TableStatus = "ACTIVE"
	return &td, nil
}

func (b *MemoryDB) PutItem(tableName string, r interface{}) error {
	if b.tables == nil {
		return errors.New("no tables")
	}
	t, ok := b.tables[tableName]
	if !ok {
		return errors.New("no such table")
	}
	v := reflect.ValueOf(r)
	pk := v.FieldByName(t.definition.KeySchema[0].AttributeName).String()
	t.items[pk] = r
	return nil
}

type mScanResponse struct {
	table *table
}

func (sr *mScanResponse) GetScannedCount() int {
	return len(sr.table.items)
}

func (sr *mScanResponse) Item(item interface{}, i int) (err error) {
	j := 0
	for _, v := range sr.table.items {
		if i == j {
			reflect.ValueOf(item).Elem().Set(reflect.ValueOf(v.(interface{})))
			return
		}
		i++
	}
	return errors.New("bad index")
}

func (b *MemoryDB) Scan(tableName string) (scanResponse ScanResponse, err error) {
	if b.tables == nil {
		return nil, errors.New("no tables")
	}
	t, ok := b.tables[tableName]
	if !ok {
		return nil, errors.New("no such table")
	}
	return &mScanResponse{t}, nil
}
