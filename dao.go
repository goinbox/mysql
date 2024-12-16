package mysql

import (
	"database/sql"

	"github.com/goinbox/pcontext"
)

type SqlQueryParams struct {
	CondItems []*SqlColQueryItem

	OrderBy string
	Offset  int64
	Cnt     int64
}

type SqlExecResult struct {
	Err          error
	LastInsertID int64
	RowsAffected int64
}

type Dao struct {
	*Client
}

func (d *Dao) Insert(ctx pcontext.Context, tableName string, colNames []string, colsValues ...[]interface{}) *SqlExecResult {
	sqb := new(SqlQueryBuilder)
	sqb.Insert(tableName, colNames...).
		Values(colsValues...)

	return ConvertSqlResultToSqlExecResult(d.Exec(ctx, sqb.Query(), sqb.Args()...))
}

func (d *Dao) queryItemForIDs(ids ...int64) *SqlColQueryItem {
	condItem := &SqlColQueryItem{
		Name:      "id",
		Condition: "",
		Value:     nil,
		NoBind:    false,
	}
	if len(ids) == 1 {
		condItem.Condition = SqlCondEqual
		condItem.Value = ids[0]
	} else {
		condItem.Condition = SqlCondIn
		condItem.Value = ids
	}

	return condItem
}

func (d *Dao) DeleteByQueryAnd(ctx pcontext.Context,
	tableName string, condItems ...*SqlColQueryItem) *SqlExecResult {
	sqb := new(SqlQueryBuilder)

	sqb.Delete(tableName).WhereConditionAnd(condItems...)

	return ConvertSqlResultToSqlExecResult(d.Exec(ctx, sqb.Query(), sqb.Args()...))
}

func (d *Dao) DeleteByIDs(ctx pcontext.Context, tableName string, ids ...int64) *SqlExecResult {
	condItem := d.queryItemForIDs(ids...)

	return d.DeleteByQueryAnd(ctx, tableName, condItem)
}

func (d *Dao) UpdateByQueryAnd(ctx pcontext.Context,
	tableName string, updateColumns []*SqlUpdateColumn, condItems ...*SqlColQueryItem) *SqlExecResult {
	sqb := new(SqlQueryBuilder)

	sqb.Update(tableName).Set(updateColumns).WhereConditionAnd(condItems...)

	return ConvertSqlResultToSqlExecResult(d.Exec(ctx, sqb.Query(), sqb.Args()...))
}

func (d *Dao) UpdateByIDs(ctx pcontext.Context,
	tableName string, updateColumns []*SqlUpdateColumn, ids ...int64) *SqlExecResult {
	condItem := d.queryItemForIDs(ids...)

	return d.UpdateByQueryAnd(ctx, tableName, updateColumns, condItem)
}

func (d *Dao) SelectByID(ctx pcontext.Context, tableName string, what string, id int64) *sql.Row {
	sqb := new(SqlQueryBuilder)
	sqb.Select(what, tableName).
		WhereConditionAnd(&SqlColQueryItem{"id", SqlCondEqual, id, false})

	return d.QueryRow(ctx, sqb.Query(), sqb.Args()...)
}

func (d *Dao) SimpleQueryOneAnd(ctx pcontext.Context,
	tableName string, what string, condItems ...*SqlColQueryItem) *sql.Row {
	sqb := new(SqlQueryBuilder)
	sqb.Select(what, tableName).
		WhereConditionAnd(condItems...)

	return d.QueryRow(ctx, sqb.Query(), sqb.Args()...)
}

func (d *Dao) SimpleQueryAnd(ctx pcontext.Context,
	tableName string, what string, params *SqlQueryParams) (*sql.Rows, error) {
	sqb := new(SqlQueryBuilder)
	sqb.Select(what, tableName).
		WhereConditionAnd(params.CondItems...).
		OrderBy(params.OrderBy).
		Limit(params.Offset, params.Cnt)

	return d.Query(ctx, sqb.Query(), sqb.Args()...)
}

func (d *Dao) SimpleTotalAnd(ctx pcontext.Context, tableName string, condItems ...*SqlColQueryItem) (int64, error) {
	sqb := new(SqlQueryBuilder)
	sqb.Select("count(*)", tableName).
		WhereConditionAnd(condItems...)

	var total int64
	err := d.QueryRow(ctx, sqb.Query(), sqb.Args()...).Scan(&total)

	return total, err
}

func ConvertSqlResultToSqlExecResult(sqlResult sql.Result, err error) *SqlExecResult {
	execResult := new(SqlExecResult)
	if err != nil {
		execResult.Err = err
	} else {
		lid, err := sqlResult.LastInsertId()
		if err != nil {
			execResult.Err = err
		} else {
			execResult.LastInsertID = lid
			ra, err := sqlResult.RowsAffected()
			if err != nil {
				execResult.Err = err
			} else {
				execResult.RowsAffected = ra
			}
		}
	}

	return execResult
}
