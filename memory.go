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

func (b *MemoryDB) CreateTable(name string, attributeDefinitions []AttributeDefinition, keySchema KeySchema) {
	definition := Table{name, keySchema, attributeDefinitions}
	if b.tables == nil {
		b.tables = make(map[string]*table)
	}
	b.tables[name] = &table{definition: &definition, items: make(map[string]interface{})}
}

func (b *MemoryDB) Put(tableName string, r interface{}) error {
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

func (b *MemoryDB) Scan(tableName string) (items []interface{}, err error) {
	if b.tables == nil {
		return nil, errors.New("no tables")
	}
	t, ok := b.tables[tableName]
	if !ok {
		return nil, errors.New("no such table")
	}
	for _, item := range t.items {
		items = append(items, item)
	}
	return
}
