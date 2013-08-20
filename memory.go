package dynamodb

import (
	"errors"
)

type items map[string]Item

type memory struct {
	Tables
	tables map[string]items
}

func NewMemoryDB() DynamoDB {
	return &memory{Tables: make(Tables)}
}

func (b *memory) CreateTable(t *Table) error {
	if b.tables == nil {
		b.tables = make(map[string]items)
	}
	b.tables[t.TableName] = make(items)
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

func (b *memory) PutItem(tableName string, item Item) error {
	if b.tables == nil {
		return errors.New("no tables")
	}
	t, ok := b.tables[tableName]
	if !ok {
		return errors.New("no such table")
	}
	pk := ""
	hash := b.Tables[tableName].KeySchema[0]
	m := item[hash.AttributeName]
	if len(m) == 1 {
		for _, v := range m {
			pk = v
		}
	} else {
		panic("boo")
	}
	t[pk] = item
	return nil
}

func (b *memory) GetItem(tableName string, key Key) (*GetItemResponse, error) {
	if b.tables == nil {
		return nil, errors.New("no tables")
	}
	t, ok := b.tables[tableName]
	if !ok {
		return nil, errors.New("no such table")
	}

	pk := ""
	hash := b.Tables[tableName].KeySchema[0]
	m := key[hash.AttributeName]
	if len(m) == 1 {
		for _, v := range m {
			pk = v
		}
	} else {
		panic("boo")
	}

	return &GetItemResponse{t[pk]}, nil
}

func (b *memory) Scan(tableName string) (scanResponse *ScanResponse, err error) {
	if b.tables == nil {
		return nil, errors.New("no tables")
	}
	t, ok := b.tables[tableName]
	if !ok {
		return nil, errors.New("no such table")
	}
	var items []Item
	for _, item := range t {
		items = append(items, item)
	}
	return &ScanResponse{Count: len(t), ScannedCount: len(t), Items: items}, nil
}

func (m *memory) Query(query *Query) (*QueryResponse, error) {
	return nil, errors.New("NYI")
}
