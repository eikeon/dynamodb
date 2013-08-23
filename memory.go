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

func (b *memory) BatchGetItem(requestedItems map[string]KeysAndAttributes, options *BatchGetItemOptions) (*BatchGetItemResult, error) {
	return nil, errors.New("NYI")
}

func (b *memory) BatchWriteItem(requestedItems map[string]WriteRequest, options *BatchWriteItemOptions) (*BatchWriteItemResult, error) {
	return nil, errors.New("NYI")
}

func (b *memory) CreateTable(tableName string, attributeDefinitions []AttributeDefinition, keySchema []KeySchemaElement, ProvisionedThroughput ProvisionedThroughput, options *CreateTableOptions) (*CreateTableResult, error) {
	if b.tables == nil {
		b.tables = make(map[string]items)
	}
	b.tables[tableName] = make(items)
	td := TableDescription{}
	td.TableStatus = "ACTIVE"
	return &CreateTableResult{TableDescription: &td}, nil
}

func (m *memory) UpdateItem(tableName string, key Key, options *UpdateItemOptions) (*UpdateItemResult, error) {
	return nil, errors.New("NYI")
}

func (db *memory) UpdateTable(tableName string, provisionedThroughput ProvisionedThroughput, options *UpdateTableOptions) (*UpdateTableResult, error) {
	return nil, errors.New("NYI")
}

func (db *memory) DescribeTable(tableName string, options *DescribeTableOptions) (*DescribeTableResult, error) {

	td := TableDescription{}
	td.TableStatus = "ACTIVE"
	return &DescribeTableResult{Table: &td}, nil
}

func (db *memory) DeleteTable(tableName string, options *DeleteTableOptions) (*DeleteTableResult, error) {
	delete(db.tables, tableName)
	td := TableDescription{}
	// TODO
	return &DeleteTableResult{TableDescription: &td}, nil
}

func (b *memory) PutItem(tableName string, item Item, options *PutItemOptions) (*PutItemResult, error) {
	if b.tables == nil {
		return nil, errors.New("no tables")
	}
	t, ok := b.tables[tableName]
	if !ok {
		return nil, errors.New("no such table")
	}
	pk := ""
	hash := b.Tables[tableName].TableDescription.KeySchema[0]
	m := item[hash.AttributeName]
	if len(m) == 1 {
		for _, v := range m {
			pk = v
		}
	} else {
		panic("boo")
	}
	t[pk] = item
	r := PutItemResult{} // TODO
	return &r, nil
}

func (b *memory) DeleteItem(tableName string, key Key, options *DeleteItemOptions) (*DeleteItemResult, error) {
	return nil, errors.New("NYI")
}

func (b *memory) GetItem(tableName string, key Key, options *GetItemOptions) (*GetItemResult, error) {
	if b.tables == nil {
		return nil, errors.New("no tables")
	}
	t, ok := b.tables[tableName]
	if !ok {
		return nil, errors.New("no such table")
	}

	pk := ""
	hash := b.Tables[tableName].TableDescription.KeySchema[0]
	m := key[hash.AttributeName]
	if len(m) == 1 {
		for _, v := range m {
			pk = v
		}
	} else {
		panic("boo")
	}
	return &GetItemResult{t[pk]}, nil
}

func (b *memory) ListTables(options *ListTablesOptions) (*ListTablesResult, error) {
	return nil, errors.New("NYI")
}

func (b *memory) Scan(tableName string, options *ScanOptions) (scanResult *ScanResult, err error) {
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
	return &ScanResult{Count: len(t), ScannedCount: len(t), Items: items}, nil
}

func (m *memory) Query(tableName string, options *QueryOptions) (*QueryResult, error) {
	return nil, errors.New("NYI")
}
