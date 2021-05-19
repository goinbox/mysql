package mysql

import (
	"database/sql"
	"reflect"
	"strings"
)

const (
	EntityMysqlFieldTag = "mysql"
)

func ReflectColNames(ret reflect.Type) []string {
	var cns []string

	for i := 0; i < ret.NumField(); i++ {
		retf := ret.Field(i)
		if retf.Type.Kind() == reflect.Struct {
			cns = ReflectColNames(retf.Type)
			continue
		}

		if name, ok := retf.Tag.Lookup(EntityMysqlFieldTag); ok {
			cns = append(cns, name)
		}
	}

	return cns
}

func ReflectColValues(rev reflect.Value) []interface{} {
	var colValues []interface{}

	ret := rev.Type()
	for i := 0; i < rev.NumField(); i++ {
		revf := rev.Field(i)
		if revf.Kind() == reflect.Struct {
			colValues = ReflectColValues(revf)
			continue
		}

		_, ok := ret.Field(i).Tag.Lookup(EntityMysqlFieldTag)
		if ok {
			colValues = append(colValues, revf.Interface())
		}
	}

	return colValues
}

func ReflectEntityScanDests(rev reflect.Value) []interface{} {
	var dests []interface{}

	ret := rev.Type()
	for i := 0; i < rev.NumField(); i++ {
		revf := rev.Field(i)
		if revf.Kind() == reflect.Struct {
			dests = ReflectEntityScanDests(revf)
			continue
		}

		_, ok := ret.Field(i).Tag.Lookup(EntityMysqlFieldTag)
		if ok {
			dests = append(dests, revf.Addr().Interface())
		}
	}

	return dests
}

func ReflectQueryRowsToEntities(rows *sql.Rows, ret reflect.Type, entitiesPtr interface{}) error {
	rlistv := reflect.ValueOf(entitiesPtr).Elem()

	for rows.Next() {
		rev := reflect.New(ret)
		dests := ReflectEntityScanDests(rev.Elem())
		err := rows.Scan(dests...)
		if err != nil {
			return err
		}
		rlistv.Set(reflect.Append(rlistv, rev))
	}

	return nil
}

type EntityDao struct {
	*Dao
}

func (ed *EntityDao) InsertEntities(tableName string, entities ...interface{}) error {
	colNames := ReflectColNames(reflect.TypeOf(entities[0]).Elem())
	colsValues := make([][]interface{}, len(entities))
	for i, item := range entities {
		colsValues[i] = ReflectColValues(reflect.ValueOf(item).Elem())
	}

	result := ed.Insert(tableName, colNames, colsValues...)
	return result.Err
}

func (ed *EntityDao) SelectEntityById(tableName string, id int64, entity interface{}) error {
	colNames := ReflectColNames(reflect.TypeOf(entity).Elem())
	row := ed.SelectById(tableName, strings.Join(colNames, ","), id)
	dests := ReflectEntityScanDests(reflect.ValueOf(entity).Elem())

	return row.Scan(dests...)
}

func (ed *EntityDao) SimpleQueryEntitiesAnd(tableName string, params *SqlQueryParams, entitiesPtr interface{}) error {
	ret := reflect.TypeOf(entitiesPtr).Elem().Elem().Elem()
	colNames := ReflectColNames(ret)
	rows, err := ed.SimpleQueryAnd(tableName, strings.Join(colNames, ","), params)
	if err != nil {
		return err
	}

	err = ReflectQueryRowsToEntities(rows, ret, entitiesPtr)
	return err
}
