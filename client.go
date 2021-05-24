package mysql

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"
)

type dbItem struct {
	config *Config
	db     *sql.DB
}

var globalDBMap = map[string]*dbItem{}

func AddGlobalDB(key string, config *Config) error {
	db, err := sql.Open("mysql", config.FormatDSN())
	if err != nil {
		return nil
	}

	globalDBMap[key] = &dbItem{
		config: config,
		db:     db,
	}

	return nil
}

type logFunc func(query []byte)

type Client struct {
	db *sql.DB
	tx *sql.Tx

	lf logFunc
}

func NewClient(key string) (*Client, error) {
	item, ok := globalDBMap[key]
	if !ok {
		return nil, errors.New("DB " + key + " not exist")
	}

	return &Client{
		db: item.db,
		tx: nil,

		lf: nil,
	}, nil
}

func (c *Client) SetLogFunc(lf logFunc) *Client {
	c.lf = lf

	return c
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
	if c.lf == nil {
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

	c.lf([]byte(fmt.Sprintf(query, vs...)))
}
