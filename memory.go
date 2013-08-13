package dynamodb

import (
	"errors"
	"reflect"
)

type table struct {
	definition *Table
	items      map[string]interface{}
}

type memory struct {
	tables map[string]*table
	types  map[string]reflect.Type
}

func NewMemoryDB() DynamoDB {
	return &memory{}
}

func (db *memory) Register(tableName string, i interface{}) {
	tableType := reflect.TypeOf(i).Elem()
	if db.types == nil {
		db.types = make(map[string]reflect.Type)
	}
	db.types[tableName] = tableType
}

func (db *memory) TableType(tableName string) reflect.Type {
	return db.types[tableName]
}

func (b *memory) CreateTable(name string) error {
	t, err := TableFor(name, b.TableType(name))
	if err != nil {
		return err
	}
	//definition := t //Table{name, keySchema, attributeDefinitions}
	if b.tables == nil {
		b.tables = make(map[string]*table)
	}
	b.tables[name] = &table{definition: t, items: make(map[string]interface{})}
	return nil
}

func (db *memory) UpdateTable(tableName string, provisionedThroughput ProvisionedThroughput) error {
	return nil
}

func (db *memory) DescribeTable(tableName string) (*TableDescription, error) {
	td := TableDescription{}
	td.Table.TableStatus = "ACTIVE"
	return &td, nil
}

func (db *memory) DeleteTable(tableName string) error {
	delete(db.tables, tableName)
	return nil
}

func (b *memory) PutItem(tableName string, r interface{}) error {
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

func (b *memory) GetItem(tableName string, key interface{}) (interface{}, error) {
	if b.tables == nil {
		return nil, errors.New("no tables")
	}
	t, ok := b.tables[tableName]
	if !ok {
		return nil, errors.New("no such table")
	}
	v := reflect.ValueOf(key).Elem()
	pk := v.FieldByName(t.definition.KeySchema[0].AttributeName).String()
	return t.items[pk], nil
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

func (b *memory) Scan(tableName string) (scanResponse ScanResponse, err error) {
	if b.tables == nil {
		return nil, errors.New("no tables")
	}
	t, ok := b.tables[tableName]
	if !ok {
		return nil, errors.New("no such table")
	}
	return &mScanResponse{t}, nil
}
