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

	row := dao.SelectByID(ctx, SQL_TEST_TABLE_NAME, "*", 1)
	err := row.Scan(&entity.ID, &entity.AddTime, &entity.EditTime, &entity.Name, &entity.Status)
	t.Log(err, entity)

	condItems := []*SqlColQueryItem{
		{"name", SqlCondLike, "%a%", false},
		{"id", SqlCondBetween, []int64{0, 100}, false},
		{"status", SqlCondEqual, 0, false},
	}
	params := &SqlQueryParams{
		CondItems: condItems,
		OrderBy:   "id desc",
		Offset:    0,
		Cnt:       10,
	}
	rows, _ := dao.SimpleQueryAnd(ctx, SQL_TEST_TABLE_NAME, "*", params)
	for rows.Next() {
		_ = rows.Scan(&entity.ID, &entity.AddTime, &entity.EditTime, &entity.Name, &entity.Status)
		t.Log(entity)
	}

	total, _ := dao.SimpleTotalAnd(ctx, SQL_TEST_TABLE_NAME, condItems...)
	t.Log(total)

	row = dao.SimpleQueryOneAnd(ctx, SQL_TEST_TABLE_NAME, "*", condItems...)
	err = row.Scan(&entity.ID, &entity.AddTime, &entity.EditTime, &entity.Name, &entity.Status)
	t.Log(err, entity)
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
	result := dao.Insert(ctx, SQL_TEST_TABLE_NAME, colNames, colsValues...)
	t.Log(result)
	if result.Err != nil {
		if DuplicateError(result.Err) {
			t.Log("DuplicateError")
		}
	}

	id := result.LastInsertID
	updateColumns := []*SqlUpdateColumn{
		{
			Name:  "name",
			Value: "abc",
		},
		{
			Name:   "status",
			Value:  "status + 1",
			NoBind: true,
		},
	}
	result = dao.UpdateByIDs(ctx, SQL_TEST_TABLE_NAME, updateColumns, id)
	t.Log(result)

	result = dao.DeleteByIDs(ctx, SQL_TEST_TABLE_NAME, id)
	t.Log(result)
}
