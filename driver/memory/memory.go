package dynamodb

import (
	"errors"
	"reflect"

	"github.com/eikeon/dynamodb"
	"github.com/eikeon/dynamodb/driver"
)

type table struct {
	definition *driver.Table
	items      map[string]interface{}
}

func init() {
	dynamodb.Register("memory", &MemoryDriver{})
}

type MemoryDriver struct {
	tables map[string]*table
	types  map[string]reflect.Type
}

func (db *MemoryDriver) Register(tableName string, tableType reflect.Type) {
	if db.types == nil {
		db.types = make(map[string]reflect.Type)
	}
	db.types[tableName] = tableType
}

func (db *MemoryDriver) TableType(tableName string) reflect.Type {
	return db.types[tableName]
}

func (b *MemoryDriver) CreateTable(name string, attributeDefinitions []driver.AttributeDefinition, keySchema driver.KeySchema, provisionedThroughput driver.ProvisionedThroughput) error {
	definition := driver.Table{name, keySchema, attributeDefinitions}
	if b.tables == nil {
		b.tables = make(map[string]*table)
	}
	b.tables[name] = &table{definition: &definition, items: make(map[string]interface{})}
	return nil
}

func (db *MemoryDriver) DescribeTable(tableName string) (*driver.TableDescription, error) {
	td := driver.TableDescription{}
	td.Table.TableStatus = "ACTIVE"
	return &td, nil
}

func (db *MemoryDriver) DeleteTable(tableName string) error {
	delete(db.tables, tableName)
	return nil
}

func (b *MemoryDriver) PutItem(tableName string, r interface{}) error {
	if b.tables == nil {
		return errors.New("no tables")
	}
	t, ok := b.tables[tableName]
	if !ok {
		return errors.New("no such table")
	}
	v := reflect.ValueOf(r).Elem()
	pk := v.FieldByName(t.definition.KeySchema[0].AttributeName).String()
	t.items[pk] = r
	return nil
}

type mScanResponse struct {
	table *table
}

func (sr *mScanResponse) GetCount() int {
	return len(sr.table.items)
}

func (sr *mScanResponse) GetScannedCount() int {
	return len(sr.table.items)
}

func (sr *mScanResponse) GetItems() (items []interface{}) {
	for _, item := range sr.table.items {
		items = append(items, item)
	}
	return
}

func (b *MemoryDriver) Scan(tableName string) (scanResponse driver.ScanResponse, err error) {
	if b.tables == nil {
		return nil, errors.New("no tables")
	}
	t, ok := b.tables[tableName]
	if !ok {
		return nil, errors.New("no such table")
	}
	return &mScanResponse{t}, nil
}
