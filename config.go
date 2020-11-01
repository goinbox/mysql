package mysql

import (
	"github.com/go-sql-driver/mysql"

	"github.com/goinbox/golog"

	"time"
)

const (
	DefaultConnectTimeout = 10 * time.Second
	DefaultReadTimeout    = 10 * time.Second
	DefaultWriteTimeout   = 10 * time.Second
)

type Config struct {
	*mysql.Config

	LogLevel int
}

func NewConfig(user, pass, host, port, dbname string) *Config {
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

		LogLevel: golog.LevelInfo,
	}
}
