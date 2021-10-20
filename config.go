package mysql

import (
	"time"

	"github.com/go-sql-driver/mysql"
)

const (
	DefaultConnectTimeout = 10 * time.Second
	DefaultReadTimeout    = 10 * time.Second
	DefaultWriteTimeout   = 10 * time.Second

	DefaultLogFieldKeyAddr = "mysql"
	DefaultLogFieldKeySql  = "sql"
)

type Config struct {
	*mysql.Config

	LogFieldKeyAddr string
	LogFieldKeySql  string
}

func NewDefaultConfig(user, pass, host, port, dbname string) *Config {
	params := map[string]string{
		"interpolateParams": "true",
	}

	config := &mysql.Config{
		User:                 user,
		Passwd:               pass,
		Net:                  "tcp",
		Addr:                 host + ":" + port,
		DBName:               dbname,
		Params:               params,
		Timeout:              DefaultConnectTimeout,
		ReadTimeout:          DefaultReadTimeout,
		WriteTimeout:         DefaultWriteTimeout,
		AllowNativePasswords: true,
	}

	return &Config{
		Config: config,

		LogFieldKeyAddr: DefaultLogFieldKeyAddr,
		LogFieldKeySql:  DefaultLogFieldKeySql,
	}
}
