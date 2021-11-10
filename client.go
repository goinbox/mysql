package mysql

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/goinbox/golog"
)

type dbItem struct {
	config *Config
	db     *sql.DB
}

var dbPool = map[string]*dbItem{}

func RegisterDB(key string, config *Config) error {
	db, err := sql.Open("mysql", config.FormatDSN())
	if err != nil {
		return nil
	}

	dbPool[key] = &dbItem{
		config: config,
		db:     db,
	}

	return nil
}

type Client struct {
	db *sql.DB
	tx *sql.Tx

	config *Config
	logger golog.Logger
}

func NewClientFromPool(key string, logger golog.Logger) (*Client, error) {
	item, ok := dbPool[key]
	if !ok {
		return nil, errors.New("DB " + key + " not exist")
	}

	return newClient(item.db, item.config, logger), nil
}

func NewClient(config *Config, logger golog.Logger) (*Client, error) {
	db, err := sql.Open("mysql", config.FormatDSN())
	if err != nil {
		return nil, err
	}

	return newClient(db, config, logger), nil
}

func newClient(db *sql.DB, config *Config, logger golog.Logger) *Client {
	client := &Client{
		db: db,
		tx: nil,

		config: config,
	}

	if logger != nil {
		client.logger = logger.With(&golog.Field{
			Key:   config.LogFieldKeyAddr,
			Value: config.Addr,
		})
	}

	return client
}

func (c *Client) Exec(query string, args ...interface{}) (sql.Result, error) {
	c.log(query, args...)

	if c.tx != nil {
		return c.tx.Exec(query, args...)
	} else {
		return c.db.Exec(query, args...)
	}
}

func (c *Client) Query(query string, args ...interface{}) (*sql.Rows, error) {
	c.log(query, args...)

	if c.tx != nil {
		return c.tx.Query(query, args...)
	} else {
		return c.db.Query(query, args...)
	}
}

func (c *Client) QueryRow(query string, args ...interface{}) *sql.Row {
	c.log(query, args...)

	if c.tx != nil {
		return c.tx.QueryRow(query, args...)
	} else {
		return c.db.QueryRow(query, args...)
	}
}

func (c *Client) Begin() error {
	tx, err := c.db.Begin()
	if err != nil {
		return err
	}

	c.log("BEGIN")
	c.tx = tx

	return nil
}

func (c *Client) Commit() error {
	defer func() {
		c.tx = nil
	}()

	if c.tx != nil {
		c.log("COMMIT")

		return c.tx.Commit()
	}

	return errors.New("Not in trans")
}

func (c *Client) Rollback() error {
	defer func() {
		c.tx = nil
	}()

	if c.tx != nil {
		c.log("ROLLBACK")

		return c.tx.Rollback()
	}

	return errors.New("Not in trans")
}

func (c *Client) log(query string, args ...interface{}) {
	if c.logger == nil {
		return
	}

	query = strings.Replace(query, "?", "%s", -1)
	vs := make([]interface{}, len(args))

	for i, v := range args {
		s := fmt.Sprint(v)
		switch v.(type) {
		case string:
			vs[i] = "'" + s + "'"
		default:
			vs[i] = s
		}
	}

	c.logger.Info("run sql", &golog.Field{
		Key:   c.config.LogFieldKeySql,
		Value: fmt.Sprintf(query, vs...),
	})
}
