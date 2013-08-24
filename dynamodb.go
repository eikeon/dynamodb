// A complete client side implementation of the DynamoDB API Version 2012-08-10 along with methods for mapping between items and go values (structs).
package dynamodb

// Represents the date and time when the table was created, in UNIX epoch time format.
type DateTime float64

type AttributeDefinition struct {
	AttributeName string
	AttributeType string
}

type AttributeValue map[string]string

type AttributeValueUpdate struct {
	Action string         `json:",omitempty"`
	Value  AttributeValue `json:",omitempty"`
}

type BatchGetItemOptions struct {
	ReturnConsumedCapacity string `json:",omitempty"`
}

type BatchGetItemResult struct {
	ConsumedCapacity *ConsumedCapacity
	Responses        map[string][]Item
	UnprocessedKeys  map[string]KeysAndAttributes
}

type BatchWriteItemOptions struct {
	ReturnConsumedCapacity      string `json:",omitempty"`
	ReturnItemCollectionMetrics string `json:",omitempty"`
}

type BatchWriteItemResult struct {
	ConsumedCapacity      *ConsumedCapacity
	ItemCollectionMetrics *ItemCollectionMetrics
	UnprocessedKeys       map[string]WriteRequest
}

type Condition struct {
	AttributeValueList []AttributeValue
	ComparisonOperator string
}

type ConsumedCapacity struct {
	CapacityUnits float64
	TableName     string
}

type CreateTableOptions struct {
	LocalSecondaryIndexes []LocalSecondaryIndex `json:",omitempty"`
}

type CreateTableResult struct {
	TableDescription *TableDescription
}

type DeleteItemOptions struct {
	Expected                    map[string]ExpectedAttributeValue `json:",omitempty"`
	ReturnConsumedCapacity      string                            `json:",omitempty"`
	ReturnItemCollectionMetrics string                            `json:",omitempty"`
	ReturnValues                string                            `json:",omitempty"`
}

type DeleteItemResult struct {
	Attributes            map[string]AttributeValue
	ConsumedCapacity      *ConsumedCapacity
	ItemCollectionMetrics *ItemCollectionMetrics
}

// There are no options for the DeleteTable action in the API Version 2012-08-10.
type DeleteTableOptions struct {
}

type DeleteRequest struct {
	Key Key
}

type DeleteTableResult struct {
	TableDescription *TableDescription
}

// There are no options for the DescribeTable action in the API Version 2012-08-10.
type DescribeTableOptions struct {
}

type DescribeTableResult struct {
	Table *TableDescription
}

type ExpectedAttributeValue struct {
	Exists *bool          `json:",omitempty"`
	Value  AttributeValue `json:",omitempty"`
}

type GetItemOptions struct {
	AttributesToGet        *[]string `json:",omitempty"`
	ConsistentRead         *bool     `json:",omitempty"`
	ReturnConsumedCapacity string    `json:",omitempty"`
}

type GetItemResult struct {
	ConsumedCapacity *ConsumedCapacity
	Item             *Item
}

// +
type Item map[string]AttributeValue

type ItemCollectionMetrics struct {
	ItemCollectionKey   Key
	SizeEstimateRangeGB *[]float64
}

// +
type Key map[string]AttributeValue

// +
type KeyConditions map[string]Condition

type KeySchemaElement struct {
	AttributeName string
	KeyType       string
}

type KeysAndAttributes struct {
}

type ListTablesOptions struct {
	ExclusiveStartTableName string `json:",omitempty"`
	Limit                   int    `json:",omitempty"`
}

type ListTablesResult struct {
	LastEvaluatedTableName string
	TableNames             []string
}

type LocalSecondaryIndex struct {
	IndexName  string
	KeySchema  []KeySchemaElement
	Projection Projection
}

type LocalSecondaryIndexDescription struct {
	IndexName      string
	IndexSizeBytes int64
	ItemCount      int64
	KeySchema      []KeySchemaElement
	Projection     *Projection
}

type Projection struct {
	NonKeyAttributes []string `json:",omitempty"`
	ProjectionType   string   `json:",omitempty"`
}

type ProvisionedThroughput struct {
	ReadCapacityUnits  int
	WriteCapacityUnits int
}

type ProvisionedThroughputDescription struct {
	LastDecreaseDateTime   DateTime
	LastIncreaseDateTime   DateTime
	NumberOfDecreasesToday int
	ReadCapacityUnits      int
	WriteCapacityUnits     int
}

type PutItemOptions struct {
	Expected                    map[string]ExpectedAttributeValue `json:",omitempty"`
	ReturnConsumedCapacity      string                            `json:",omitempty"`
	ReturnItemCollectionMetrics string                            `json:",omitempty"`
	ReturnValues                string                            `json:",omitempty"`
}

type PutItemResult struct {
	Attributes            map[string]AttributeValue
	ConsumedCapacity      *ConsumedCapacity
	ItemCollectionMetrics *ItemCollectionMetrics
}

type PutRequest struct {
	Item Item
}

type QueryOptions struct {
	AttributesToGet        []string      `json:",omitempty"`
	ConsistentRead         bool          `json:",omitempty"`
	ExclusiveStartKey      Key           `json:",omitempty"`
	IndexName              string        `json:",omitempty"`
	KeyConditions          KeyConditions `json:",omitempty"`
	Limit                  int           `json:",omitempty"`
	ReturnConsumedCapacity string        `json:",omitempty"`
	ScanIndexForward       *bool         `json:",omitempty"` // defaults to true
	Select                 string        `json:",omitempty"`
}

type QueryResult struct {
	ConsumedCapacity *ConsumedCapacity
	Count            int
	Items            []Item
	LastEvaluatedKey Key
}

type ScanOptions struct {
	AttributesToGet        []string      `json:",omitempty"`
	ExclusiveStartKey      Key           `json:",omitempty"`
	Limit                  int           `json:",omitempty"`
	ReturnConsumedCapacity string        `json:",omitempty"`
	ScanFilter             KeyConditions `json:",omitempty"`
	Segment                int           `json:",omitempty"`
	Select                 string        `json:",omitempty"`
	TotalSegments          int           `json:",omitempty"`
}

type ScanResult struct {
	ConsumedCapacity *ConsumedCapacity
	Count            int
	Items            []Item
	LastEvaluatedKey Key
	ScannedCount     int
}

type TableDescription struct {
	AttributeDefinitions  []AttributeDefinition
	CreationDateTime      DateTime
	ItemCount             int64
	KeySchema             []KeySchemaElement
	LocalSecondaryIndexes []LocalSecondaryIndexDescription
	ProvisionedThroughput *ProvisionedThroughputDescription
	TableName             string
	TableSizeBytes        int64
	TableStatus           string
}

type UpdateItemOptions struct {
	AttributeUpdates            map[string]AttributeValueUpdate   `json:",omitempty"`
	Expected                    map[string]ExpectedAttributeValue `json:",omitempty"`
	ReturnConsumedCapacity      string                            `json:",omitempty"`
	ReturnItemCollectionMetrics string                            `json:",omitempty"`
	ReturnValues                string                            `json:",omitempty"`
}

type UpdateItemResult struct {
	Attributes            map[string]AttributeValue
	ConsumedCapacity      *ConsumedCapacity
	ItemCollectionMetrics *ItemCollectionMetrics
}

// There are no options for the UpdateTable action in the API Version 2012-08-10.
type UpdateTableOptions struct {
}

type UpdateTableResult struct {
	TableDescription *TableDescription
}

type WriteRequest struct {
	DeleteRequest *DeleteRequest `json:",omitempty"`
	PutRequest    *PutRequest    `json:",omitempty"`
}

type DynamoDB interface {
	Mapping

	BatchGetItem(requestedItems map[string]KeysAndAttributes, options *BatchGetItemOptions) (*BatchGetItemResult, error)
	BatchWriteItem(requestedItems map[string]WriteRequest, options *BatchWriteItemOptions) (*BatchWriteItemResult, error)
	CreateTable(tableName string, attributeDefinitions []AttributeDefinition, keySchema []KeySchemaElement, ProvisionedThroughput ProvisionedThroughput, options *CreateTableOptions) (*CreateTableResult, error)
	DeleteItem(tableName string, key Key, options *DeleteItemOptions) (*DeleteItemResult, error)
	DeleteTable(tableName string, options *DeleteTableOptions) (*DeleteTableResult, error)
	DescribeTable(tableName string, options *DescribeTableOptions) (*DescribeTableResult, error)
	GetItem(tableName string, key Key, options *GetItemOptions) (*GetItemResult, error)
	ListTables(options *ListTablesOptions) (*ListTablesResult, error)
	PutItem(tableName string, item Item, options *PutItemOptions) (*PutItemResult, error)
	Query(tableName string, options *QueryOptions) (*QueryResult, error)
	Scan(tableName string, options *ScanOptions) (*ScanResult, error)
	UpdateItem(tableName string, key Key, options *UpdateItemOptions) (*UpdateItemResult, error)
	UpdateTable(tableName string, provisionedThroughput ProvisionedThroughput, options *UpdateTableOptions) (*UpdateTableResult, error)
}
