package mysql

import (
	"fmt"
	"testing"
)

const TableName = "demo"

var sqb SqlQueryBuilder

func TestSQBInsert(t *testing.T) {
	sqb.Insert(TableName, "id", "add_time", "edit_time", "name")

	printQueryAndArgs()
}

func TestSQBValues(t *testing.T) {
	sqb.Values(
		[]interface{}{1, "2016-06-23 09:00:00", "2016-06-23 09:00:00", "a"},
		[]interface{}{2, "2016-06-23 09:10:00", "2016-06-23 09:10:00", "b"},
	)

	printQueryAndArgs()
}

func TestSQBDelete(t *testing.T) {
	sqb.Delete(TableName)

	printQueryAndArgs()
}

func TestSQBUpdate(t *testing.T) {
	sqb.Update(TableName)

	printQueryAndArgs()
}

func TestSQBSet(t *testing.T) {
	sqb.Set(map[string]interface{}{
		"name":      "d",
		"edit_time": "2016-06-24 09:00:00",
	})

	printQueryAndArgs()
}

func TestSQBSelect(t *testing.T) {
	sqb.Select("*", TableName)
	printQueryAndArgs()

	sqb.Select("name, count(*)", TableName)
	printQueryAndArgs()
}

func TestSQBWhere(t *testing.T) {
	sqb.WhereConditionAnd(
		//NewSqlColQueryItem("id", SqlCondIn, []int64{1, 2}),
		//NewSqlColQueryItem("add_time", SqlCondBetween, []string{"2016-06-23 00:00:00", "2016-06-25 00:00:00"}),
		&SqlColQueryItem{"edit_time", SqlCondLessEqual, "2016-06-24 09:00:00"},
		//NewSqlColQueryItem("name", SqlCondLike, "%a%"),
	)
	printQueryAndArgs()
}

func TestSQBGroupBy(t *testing.T) {
	sqb.GroupBy("name ASC")
	printQueryAndArgs()
}

func TestSQBHaving(t *testing.T) {
	sqb.HavingConditionAnd(
		&SqlColQueryItem{"id", SqlCondGreater, 3},
	)
	printQueryAndArgs()
}

func TestSQBOrderBy(t *testing.T) {
	sqb.OrderBy("id DESC")
	printQueryAndArgs()
}

func TestSQBLimit(t *testing.T) {
	sqb.Limit(0, 10)
	printQueryAndArgs()
}

func printQueryAndArgs() {
	fmt.Println(sqb.Query(), sqb.Args())
}
