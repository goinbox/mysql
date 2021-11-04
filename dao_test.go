package mysql

import (
	"github.com/goinbox/gomisc"

	"testing"
	"time"
)

const (
	SQL_TEST_TABLE_NAME = "demo"
)

func TestDaoRead(t *testing.T) {
	dao := &Dao{client}
	entity := new(demoEntity)

	row := dao.SelectById(SQL_TEST_TABLE_NAME, "*", 1)
	_ = row.Scan(&entity.ID, &entity.AddTime, &entity.EditTime, &entity.Name, &entity.Status)
	t.Log(entity)

	condItems := []*SqlColQueryItem{
		{"name", SqlCondLike, "%a%"},
		{"id", SqlCondBetween, []int64{0, 100}},
		{"status", SqlCondEqual, 0},
	}
	params := &SqlQueryParams{
		CondItems: condItems,
		OrderBy:   "id desc",
		Offset:    0,
		Cnt:       10,
	}
	rows, _ := dao.SimpleQueryAnd(SQL_TEST_TABLE_NAME, "*", params)
	for rows.Next() {
		_ = rows.Scan(&entity.ID, &entity.AddTime, &entity.EditTime, &entity.Name, &entity.Status)
		t.Log(entity)
	}

	total, _ := dao.SimpleTotalAnd(SQL_TEST_TABLE_NAME, condItems...)
	t.Log(total)
}

func TestDaoWrite(t *testing.T) {
	dao := &Dao{client}

	var colNames = []string{"id", "add_time", "edit_time", "name", "status"}
	var colsValues [][]interface{}

	ts := time.Now().Format(gomisc.TimeGeneralLayout())
	for i, name := range []string{"a", "b", "c"} {
		colValues := []interface{}{
			int64(i + 10),
			ts,
			ts,
			name,
			i % 10,
		}
		colsValues = append(colsValues, colValues)
	}
	result := dao.Insert(SQL_TEST_TABLE_NAME, colNames, colsValues...)
	t.Log(result)
	if result.Err != nil {
		if DuplicateError(result.Err) {
			t.Log("DuplicateError")
		}
	}

	id := result.LastInsertId
	updateFields := map[string]interface{}{
		"name":      "abc",
		"edit_time": ts,
	}
	result = dao.UpdateByIds(SQL_TEST_TABLE_NAME, updateFields, id)
	t.Log(result)

	result = dao.DeleteByIds(SQL_TEST_TABLE_NAME, id)
	t.Log(result)
}
