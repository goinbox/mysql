package mysql

import (
	"database/sql"
	"github.com/go-sql-driver/mysql"
)

type SqlQueryParams struct {
	CondItems []*SqlColQueryItem

	OrderBy string
	Offset  int64
	Cnt     int64
}

type SqlExecResult struct {
	Err          error
	LastInsertId int64
	RowsAffected int64
}

type Dao struct {
	*Client
}

func (d *Dao) Insert(tableName string, colNames []string, colsValues ...[]interface{}) *SqlExecResult {
	sqb := new(SqlQueryBuilder)
	sqb.Insert(tableName, colNames...).
		Values(colsValues...)

	return ConvertSqlResultToSqlExecResult(d.Exec(sqb.Query(), sqb.Args()...))
}

func (d *Dao) DeleteById(tableName string, id int64) *SqlExecResult {
	sqb := new(SqlQueryBuilder)
	sqb.Delete(tableName).
		WhereConditionAnd(&SqlColQueryItem{"id", SqlCondEqual, id})

	return ConvertSqlResultToSqlExecResult(d.Exec(sqb.Query(), sqb.Args()...))
}

func (d *Dao) UpdateById(tableName string, id int64, updateFields map[string]interface{}) *SqlExecResult {
	sqb := new(SqlQueryBuilder)
	sqb.Update(tableName).
		Set(updateFields).
		WhereConditionAnd(&SqlColQueryItem{"id", SqlCondEqual, id})

	return ConvertSqlResultToSqlExecResult(d.Exec(sqb.Query(), sqb.Args()...))
}

func (d *Dao) SelectById(tableName string, what string, id int64) *sql.Row {
	sqb := new(SqlQueryBuilder)
	sqb.Select(what, tableName).
		WhereConditionAnd(&SqlColQueryItem{"id", SqlCondEqual, id})

	return d.QueryRow(sqb.Query(), sqb.Args()...)
}

func (d *Dao) SimpleQueryAnd(tableName string, what string, params *SqlQueryParams) (*sql.Rows, error) {
	sqb := new(SqlQueryBuilder)
	sqb.Select(what, tableName).
		WhereConditionAnd(params.CondItems...).
		OrderBy(params.OrderBy).
		Limit(params.Offset, params.Cnt)

	return d.Query(sqb.Query(), sqb.Args()...)
}

func (d *Dao) SimpleTotalAnd(tableName string, condItems ...*SqlColQueryItem) (int64, error) {
	sqb := new(SqlQueryBuilder)
	sqb.Select("count(1)", tableName).
		WhereConditionAnd(condItems...)

	var total int64
	err := d.QueryRow(sqb.Query(), sqb.Args()...).Scan(&total)

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
			execResult.LastInsertId = lid
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

func DuplicateError(err error) bool {
	if err == nil {
		return false
	}

	mysqlError, ok := err.(*mysql.MySQLError)
	if ok {
		// mariadb-10.5.9/libmariadb/include/mysqld_error.h:69:#define ER_DUP_ENTRY 1062
		if mysqlError.Number == 1062 {
			return true
		}
	}

	return false
}
