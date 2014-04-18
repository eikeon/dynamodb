package dynamodb

import (
	"errors"
	"fmt"
	"log"
	"reflect"
	"strconv"
)

type Mapping interface {
	Register(tableName string, i interface{}) (*TableDescription, error)
	ToItem(s interface{}) Item
	ToKey(s interface{}) Key
	FromItem(tableName string, item Item) interface{}
}

type mapping map[string]struct {
	TableDescription *TableDescription
	TableType        reflect.Type
}

func (m mapping) Register(tableName string, i interface{}) (*TableDescription, error) {
	tableType := reflect.TypeOf(i).Elem()
	if t, err := m.tableFor(tableName, tableType); err == nil {
		m[tableName] = struct {
			TableDescription *TableDescription
			TableType        reflect.Type
		}{TableDescription: t, TableType: tableType}
		return t, nil
	} else {
		return nil, err
	}

}

func (m mapping) tableFor(tableName string, tableType reflect.Type) (*TableDescription, error) {
	var primaryHash, primaryRange *KeySchemaElement
	var attributeDefinitions []AttributeDefinition
	var keySchema []KeySchemaElement
	provisionedThroughput := ProvisionedThroughputDescription{ReadCapacityUnits: 1, WriteCapacityUnits: 1}

	for i := 0; i < tableType.NumField(); i++ {
		f := tableType.Field(i)
		attributeType := ""
		switch f.Type.Kind() {
		case reflect.String:
			attributeType = "S"
		case reflect.Int, reflect.Int64:
			attributeType = "N"
		default:
			return nil, errors.New("attribute type not supported")
		}
		name := tableType.Field(i).Name

		tag := f.Tag.Get("db")
		if tag == "HASH" {
			attributeDefinitions = append(attributeDefinitions, AttributeDefinition{name, attributeType})
			primaryHash = &KeySchemaElement{name, "HASH"}
		}
		if tag == "RANGE" {
			attributeDefinitions = append(attributeDefinitions, AttributeDefinition{name, attributeType})
			primaryRange = &KeySchemaElement{name, "RANGE"}
		}
	}

	if primaryHash == nil {
		return nil, errors.New("no primary key hash specified")
	} else {
		keySchema = append(keySchema, *primaryHash)
	}
	if primaryRange != nil {
		keySchema = append(keySchema, *primaryRange)
	}
	return &TableDescription{TableName: tableName, KeySchema: keySchema, AttributeDefinitions: attributeDefinitions, ProvisionedThroughput: &provisionedThroughput}, nil
}

func (m mapping) ToItem(s interface{}) Item {
	var it Item = make(map[string]AttributeValue)
	sValue := reflect.ValueOf(s).Elem()
	typeOfItem := sValue.Type()

	for i := 0; i < sValue.NumField(); i++ {
		f := sValue.Field(i)
		name := typeOfItem.Field(i).Name
		switch f.Type().Kind() {
		case reflect.String:
			v := f.Interface().(string)
			if v != "" {
				it[name] = map[string]string{"S": v}
			}
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			s := strconv.FormatInt(f.Int(), 10)
			it[name] = map[string]string{"N": s}
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
			s := strconv.FormatUint(f.Uint(), 10)
			it[name] = map[string]string{"N": s}
		default:
			panic("attribute type not supported")
		}

	}
	return it
}

func (m mapping) ToKey(s interface{}) Key {

	key := make(Key)

	sType := reflect.TypeOf(s).Elem()
	sValue := reflect.ValueOf(s).Elem()

	for i := 0; i < sValue.NumField(); i++ {
		sf := sType.Field(i)
		tag := sf.Tag.Get("db")
		if tag == "HASH" || tag == "RANGE" {
			fv := sValue.Field(i)
			switch sf.Type.Kind() {
			case reflect.String:
				key[sf.Name] = AttributeValue{"S": fv.Interface().(string)}
			case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
				key[sf.Name] = AttributeValue{"N": strconv.FormatInt(fv.Int(), 10)}
			default:
				panic("attribute type not supported")
			}
		}
	}
	return key
}

func (m mapping) FromItem(tableName string, item Item) interface{} {
	et := m[tableName].TableType
	v := reflect.New(et)
	v = v.Elem()
	switch v.Kind() {
	case reflect.Struct:
		for kk, vv := range item {
			if value, ok := vv["S"]; ok {
				f := v.FieldByName(kk)
				if f.CanSet() {
					f.SetString(value)
				} else {
					log.Println("Warning: can't set:", kk)
				}
			}
			if value, ok := vv["N"]; ok {
				f := v.FieldByName(kk)
				n, err := strconv.ParseInt(value, 10, 64)
				if err != nil || f.OverflowInt(n) {
					panic(fmt.Sprintf("%v %v\n", value, v.Type()))
				}
				f.SetInt(n)
			}
		}
	default:
		panic("Unsupported item type error")
	}
	return v.Addr().Interface()
}
