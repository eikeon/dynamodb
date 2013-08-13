// dynamodo...
package dynamodb

import (
	"errors"
	"fmt"
	"reflect"

	"github.com/eikeon/dynamodb/driver"
)

// For the bits copied from Golang's database/sql/
// Copyright 2011 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

var drivers = make(map[string]driver.Driver)

// Register makes a database driver available by the provided name.
// If Register is called twice with the same name or if driver is nil,
// it panics.
func Register(name string, driver driver.Driver) {
	if driver == nil {
		panic("sql: Register driver is nil")
	}
	if _, dup := drivers[name]; dup {
		panic("sql: Register called twice for driver " + name)
	}
	drivers[name] = driver
}

type DynamoDB struct {
	driver driver.Driver
}

func Open(driverName string) (*DynamoDB, error) {
	driveri, ok := drivers[driverName]
	if !ok {
		return nil, fmt.Errorf("dynamodb: unknown driver %q (forgotten import?)", driverName)
	}
	return &DynamoDB{driveri}, nil
}

func (db *DynamoDB) Register(name string, i interface{}) {
	db.driver.Register(name, reflect.TypeOf(i).Elem())
}

func (db *DynamoDB) CreateTable(tableName string) error {
	var primaryHash, primaryRange *driver.KeySchemaElement
	var attributeDefinitions []driver.AttributeDefinition
	var keySchema driver.KeySchema
	provisionedThroughput := driver.ProvisionedThroughput{1, 1}

	s := db.driver.TableType(tableName)
	//fmt.Println("s:", s)

	for i := 0; i < s.NumField(); i++ {
		f := s.Field(i)
		attributeType := ""
		switch f.Type.Kind() {
		case reflect.String:
			attributeType = "S"
		default:
			return errors.New("attribute type not supported")
		}
		name := s.Field(i).Name
		attributeDefinitions = append(attributeDefinitions, driver.AttributeDefinition{name, attributeType})

		tag := f.Tag.Get("db")
		if tag == "HASH" {
			primaryHash = &driver.KeySchemaElement{name, "HASH"}
		}
	}

	if primaryHash == nil {
		return errors.New("no primary key hash specified")
	} else {
		keySchema = append(keySchema, *primaryHash)
	}
	if primaryRange != nil {
		keySchema = append(keySchema, *primaryRange)
	}
	return db.driver.CreateTable(tableName, attributeDefinitions, keySchema, provisionedThroughput)
}

func (db *DynamoDB) DescribeTable(tableName string) (*driver.TableDescription, error) {
	return db.driver.DescribeTable(tableName)
}

func (db *DynamoDB) DeleteTable(tableName string) error {
	return db.driver.DeleteTable(tableName)
}

func (db *DynamoDB) PutItem(tableName string, item interface{}) error {
	return db.driver.PutItem(tableName, item)
}

func (db *DynamoDB) Scan(tableName string) (driver.ScanResponse, error) {
	return db.driver.Scan(tableName)
}
