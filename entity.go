package mysql

import (
	"database/sql"
	"reflect"
	"regexp"
	"strings"
)

const (
	EntityTagColumn = "column"
	EntityColumnSep = "_"
)

var (
	entityFieldRegex = regexp.MustCompile("([A-Z][a-z0-9]*)")
)

func columnByField(field *reflect.StructField) string {
	name, ok := field.Tag.Lookup(EntityTagColumn)
	if ok {
		return name
	}

	matches := entityFieldRegex.FindAllStringSubmatch(field.Name, -1)
	elems := make([]string, len(matches))
	for i, match := range matches {
		elems[i] = strings.ToLower(match[1])
	}

	return strings.Join(elems, EntityColumnSep)
}

func ReflectColNamesByType(ret reflect.Type) []string {
	var cns []string

	for i := 0; i < ret.NumField(); i++ {
		retf := ret.Field(i)
		ftype := retf.Type
		if ftype.Kind() == reflect.Ptr {
			ftype = ftype.Elem()
		}

		if ftype.Kind() == reflect.Struct {
			if ftype.String() != "time.Time" {
				cns = append(cns, ReflectColNamesByType(ftype)...)
				continue
			}
		}

		cns = append(cns, columnByField(&retf))
	}

	return cns
}

func ReflectColNamesByValue(rev reflect.Value, filterNil bool) []string {
	var cns []string

	ret := rev.Type()
	for i := 0; i < rev.NumField(); i++ {
		revf := rev.Field(i)
		if revf.Kind() == reflect.Ptr {
			if filterNil && revf.IsNil() {
				continue
			}
			revf = revf.Elem()
		}

		if revf.Kind() == reflect.Struct {
			if revf.Type().String() != "time.Time" {
				cns = append(cns, ReflectColNamesByValue(revf, filterNil)...)
				continue
			}
		}

		retf := ret.Field(i)
		cns = append(cns, columnByField(&retf))
	}

	return cns
}

func ReflectColValues(rev reflect.Value, filterNil bool) []interface{} {
	var colValues []interface{}

	for i := 0; i < rev.NumField(); i++ {
		revf := rev.Field(i)
		if revf.Kind() == reflect.Ptr {
			if filterNil && revf.IsNil() {
				continue
			}
			revf = revf.Elem()
		}
		if revf.Kind() == reflect.Struct {
			if revf.Type().String() != "time.Time" {
				colValues = append(colValues, ReflectColValues(revf, filterNil)...)
				continue
			}
		}

		colValues = append(colValues, revf.Interface())
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

		_, ok := ret.Field(i).Tag.Lookup(EntityTagColumn)
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
	Dao
}

func (d *EntityDao) InsertEntities(tableName string, entities ...interface{}) *SqlExecResult {
	colNames := ReflectColNamesByValue(reflect.ValueOf(entities[0]).Elem(), true)
	colsValues := make([][]interface{}, len(entities))
	for i, item := range entities {
		colsValues[i] = ReflectColValues(reflect.ValueOf(item).Elem(), true)
	}

	return d.Insert(tableName, colNames, colsValues...)
}

func (d *EntityDao) SelectEntityByID(tableName string, id int64, entity interface{}) error {
	colNames := ReflectColNamesByValue(reflect.ValueOf(entity).Elem(), false)
	row := d.SelectByID(tableName, strings.Join(colNames, ","), id)
	dests := ReflectEntityScanDests(reflect.ValueOf(entity).Elem())

	return row.Scan(dests...)
}

func (d *EntityDao) SimpleQueryEntitiesAnd(tableName string, params *SqlQueryParams, entitiesPtr interface{}) error {
	ret := reflect.TypeOf(entitiesPtr).Elem().Elem().Elem()
	colNames := ReflectColNamesByType(ret)
	rows, err := d.SimpleQueryAnd(tableName, strings.Join(colNames, ","), params)
	if err != nil {
		return err
	}

	err = ReflectQueryRowsToEntities(rows, ret, entitiesPtr)
	return err
}
