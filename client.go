package mysql

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/goinbox/golog"
	"github.com/goinbox/pcontext"
)

type dbItem struct {
	config *Config
	db     *sql.DB
}

var dbPool = map[string]*dbItem{}

func newDB(config *Config) (*sql.DB, error) {
	db, err := sql.Open("mysql", config.FormatDSN())
	if err != nil {
		return nil, fmt.Errorf("newDB error: %w", err)
	}

	db.SetConnMaxLifetime(config.ConnMaxLifetime)

	return db, nil
}

func RegisterDB(key string, config *Config) error {
	db, err := newDB(config)
	if err != nil {
		return nil
	}

	db.SetConnMaxLifetime(time.Second * 30)
	dbPool[key] = &dbItem{
		config: config,
		db:     db,
	}

	return nil
}

type PrepareQueryFunc func(query string, args ...interface{}) (string, []interface{})

type Client struct {
	db *sql.DB
	tx *sql.Tx

	config *Config

	prepareQuery PrepareQueryFunc
}

func NewClientFromPool(key string) (*Client, error) {
	item, ok := dbPool[key]
	if !ok {
		return nil, errors.New("DB " + key + " not exist")
	}

	return newClient(item.db, item.config), nil
}

func NewClient(config *Config) (*Client, error) {
	db, err := newDB(config)
	if err != nil {
		return nil, fmt.Errorf("newDB error: %w", err)
	}

	return newClient(db, config), nil
}

func newClient(db *sql.DB, config *Config) *Client {
	client := &Client{
		db: db,
		tx: nil,

		config: config,
	}

	return client
}

func (c *Client) SetPrepareQuery(f PrepareQueryFunc) *Client {
	c.prepareQuery = f

	return c
}

func (c *Client) Exec(ctx pcontext.Context, query string, args ...interface{}) (sql.Result, error) {
	if c.prepareQuery != nil {
		query, args = c.prepareQuery(query, args...)
	}
	c.log(ctx.Logger(), query, args...)

	if c.tx != nil {
		return c.tx.Exec(query, args...)
	}
	return c.db.ExecContext(ctx, query, args...)
}

func (c *Client) Query(ctx pcontext.Context, query string, args ...interface{}) (*sql.Rows, error) {
	if c.prepareQuery != nil {
		query, args = c.prepareQuery(query, args...)
	}
	c.log(ctx.Logger(), query, args...)

	if c.tx != nil {
		return c.tx.Query(query, args...)
	}
	return c.db.QueryContext(ctx, query, args...)
}

func (c *Client) QueryRow(ctx pcontext.Context, query string, args ...interface{}) *sql.Row {
	if c.prepareQuery != nil {
		query, args = c.prepareQuery(query, args...)
	}
	c.log(ctx.Logger(), query, args...)

	if c.tx != nil {
		return c.tx.QueryRow(query, args...)
	}
	return c.db.QueryRowContext(ctx, query, args...)
}

func (c *Client) Begin(ctx pcontext.Context) error {
	if c.tx != nil {
		return errors.New("already in trans")
	}

	tx, err := c.db.Begin()
	if err != nil {
		return err
	}

	c.log(ctx.Logger(), "BEGIN")
	c.tx = tx

	return nil
}

func (c *Client) Commit(ctx pcontext.Context) error {
	defer func() {
		c.tx = nil
	}()

	if c.tx != nil {
		c.log(ctx.Logger(), "COMMIT")

		return c.tx.Commit()
	}

	return errors.New("not in trans")
}

func (c *Client) Rollback(ctx pcontext.Context) error {
	defer func() {
		c.tx = nil
	}()

	if c.tx != nil {
		c.log(ctx.Logger(), "ROLLBACK")

		return c.tx.Rollback()
	}

	return errors.New("not in trans")
}

func (c *Client) log(logger golog.Logger, query string, args ...interface{}) {
	if logger == nil {
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

	logger.Info("run sql", &golog.Field{
		Key:   c.config.LogFieldKeySql,
		Value: fmt.Sprintf(query, vs...),
	})
}
