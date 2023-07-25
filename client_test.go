package mysql

import (
	"database/sql"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/goinbox/golog"
)

var client *Client

type tableDemoRowItem struct {
	ID       int64
	AddTime  string
	EditTime string
	Name     string
	Status   int
}

func init() {
	w, _ := golog.NewFileWriter("/dev/stdout", 0)
	logger := golog.NewSimpleLogger(w, golog.NewSimpleFormater())

	config := NewDefaultConfig("root", "123", "127.0.0.1", "gobox-demo", 3306)
	client, _ = NewClient(config, logger)
	client.SetPrepareQuery(func(query string, args ...interface{}) (string, []interface{}) {
		query = fmt.Sprintf("/*prepare query*/ %s", query)
		return query, args
	})
	// client.Exec("DELETE FROM demo")
}

func TestClientExec(t *testing.T) {
	result, err := client.Exec("INSERT INTO demo (name) VALUES (?),(?)", "a", "b")
	if err != nil {
		t.Error("exec error: " + err.Error())
	} else {
		li, err := result.LastInsertId()
		if err != nil {
			t.Error("lastInsertID error: " + err.Error())
		} else {
			t.Log("lastInsertID: " + strconv.FormatInt(li, 10))
		}

		rf, err := result.RowsAffected()
		if err != nil {
			t.Error("rowsAffected error: " + err.Error())
		} else {
			t.Log("rowsAffected: " + strconv.FormatInt(rf, 10))
		}
	}
}

func TestClientQuery(t *testing.T) {
	rows, err := client.Query("SELECT * FROM demo WHERE name IN (?,?)", "a", "b")
	if err != nil {
		t.Error("query error: " + err.Error())
	} else {
		for rows.Next() {
			item := new(tableDemoRowItem)
			err = rows.Scan(&item.ID, &item.AddTime, &item.EditTime, &item.Name, &item.Status)
			if err != nil {
				t.Error("rows scan error: " + err.Error())
			} else {
				t.Log(item)
			}
		}
	}
}

func TestClientQueryRow(t *testing.T) {
	row := client.QueryRow("SELECT * FROM demo WHERE name = ?", "a")
	item := new(tableDemoRowItem)
	err := row.Scan(&item.ID, &item.AddTime, &item.EditTime, &item.Name, &item.Status)
	if err != nil {
		if err == sql.ErrNoRows {
			t.Log("no rows: " + err.Error())
		} else {
			t.Error("row scan error: " + err.Error())
		}
	} else {
		t.Log(item)
	}
}

func TestClientTrans(t *testing.T) {
	_ = client.Begin()

	_, err := client.Exec("insert into demo (name) values ('ab')")
	_, err = client.Exec("insert into id_gen (name) values ('demo')")

	_ = client.Commit()

	// err = client.Rollback()
	t.Log(err)

	_ = client.Begin()
	_, _ = client.Exec("update id_gen set max_id = 100")
	r, err := client.Exec("update demo set name = 'abc' where id = 0")
	t.Log(err)
	n, err := r.RowsAffected()
	t.Log(n, err)
	if n == 0 {
		_ = client.Rollback()
	}
}

func TestClientPool(t *testing.T) {
	key := "test"
	_ = RegisterDB(key, NewDefaultConfig("root", "123", "127.0.0.1", "gobox-demo", 3306))

	w, _ := golog.NewFileWriter("/dev/stdout", 0)
	logger := golog.NewSimpleLogger(w, golog.NewSimpleFormater())
	client, _ = NewClientFromPool(key, logger)

	_, err := client.Exec("update demo set status = 1")
	t.Log(err)

	time.Sleep(time.Minute * 5)
	_, err = client.Exec("update demo set status = 1")
	t.Log(err)
}
