package mysql

import (
	"database/sql"

	"github.com/go-sql-driver/mysql"
)

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

func NoRowsError(err error) bool {
	if err == sql.ErrNoRows {
		return true
	}

	return false
}
