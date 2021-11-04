package mysql

import (
	"strconv"
	"testing"
	"time"
)

type demoEntity struct {
	ID       *int64 `column:"id"`
	AddTime  *time.Time
	EditTime *time.Time

	Name   string
	Status int
}

func TestInsertEntities(t *testing.T) {
	cnt := 3
	entities := make([]interface{}, cnt)
	now := time.Now()
	for i := 0; i < cnt; i++ {
		entities[i] = &demoEntity{
			AddTime: &now,
			Name:    "demo" + strconv.Itoa(i),
			Status:  0,
		}
	}

	err := entityDao().InsertEntities("demo", entities...)
	t.Log(err)
}

func TestSelectEntityByID(t *testing.T) {
	entity := new(demoEntity)
	err := entityDao().SelectEntityByID("demo", 100, entity)
	t.Log(err, entity, NoRowsError(err))
	if err == nil {
		t.Log(*entity.ID, *entity.AddTime, *entity.EditTime)
	}
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
		t.Log(i, entity, *entity.ID, *entity.AddTime, *entity.EditTime)
	}
}

func entityDao() *EntityDao {
	return &EntityDao{Dao{client}}
}
