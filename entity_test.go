package mysql

import (
	"strconv"
	"testing"
)

type demoEntity struct {
	Id       int64  `mysql:"id"`
	AddTime  string `mysql:"add_time"`
	EditTime string `mysql:"edit_time"`

	Name   string `mysql:"name"`
	Status int    `mysql:"status"`
}

func TestInsertEntities(t *testing.T) {
	cnt := 3
	entities := make([]interface{}, cnt)
	for i := 0; i < cnt; i++ {
		entities[i] = &demoEntity{
			Id:       100 + int64(i),
			AddTime:  "2021-05-19 11:25:03",
			EditTime: "2021-05-19 11:25:03",
			Name:     "demo" + strconv.Itoa(i),
			Status:   0,
		}
	}

	err := entityDao().InsertEntities("demo", entities...)
	t.Log(err)
}

func TestSelectEntityById(t *testing.T) {
	entity := new(demoEntity)
	err := entityDao().SelectEntityById("demo", 100, entity)
	t.Log(err, entity, NoRowsError(err))
}

func TestSimpleQueryEntitiesAnd(t *testing.T) {
	var entityList []*demoEntity
	condItems := []*SqlColQueryItem{
		{"name", SqlCondLike, "%demo%"},
	}
	params := &SqlQueryParams{
		CondItems: condItems,
		OrderBy:   "id desc",
		Offset:    0,
		Cnt:       10,
	}
	err := entityDao().SimpleQueryEntitiesAnd("demo", params, &entityList)
	t.Log(err, NoRowsError(err))
	for i, entity := range entityList {
		t.Log(i, entity)
	}
}

func entityDao() *EntityDao {
	return &EntityDao{Dao{client}}
}
