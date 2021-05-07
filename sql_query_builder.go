package mysql

import (
	"reflect"
	"strings"
)

const (
	SqlCondEqual        = "="
	SqlCondNotEqual     = "!="
	SqlCondLess         = "<"
	SqlCondLessEqual    = "<="
	SqlCondGreater      = ">"
	SqlCondGreaterEqual = ">="
	SqlCondIn           = "in"
	SqlCondNotIn        = "not in"
	SqlCondLike         = "like"
	SqlCondBetween      = "between"
)

type SqlColQueryItem struct {
	Name      string
	Condition string
	Value     interface{}
}

func NewSqlColQueryItem(name, condition string, value interface{}) *SqlColQueryItem {
	return &SqlColQueryItem{
		Name:      name,
		Condition: condition,
		Value:     value,
	}
}

type SqlQueryBuilder struct {
	query string
	args  []interface{}
}

func (s *SqlQueryBuilder) Query() string {
	return s.query
}

func (s *SqlQueryBuilder) Args() []interface{} {
	return s.args
}

func (s *SqlQueryBuilder) Insert(tableName string, colNames ...string) *SqlQueryBuilder {
	s.args = nil

	s.query = "INSERT INTO " + tableName + " ("
	s.query += strings.Join(colNames, ", ") + ")"

	return s
}

func (s *SqlQueryBuilder) Values(colsValues ...[]interface{}) *SqlQueryBuilder {
	l := len(colsValues) - 1
	if l == -1 {
		return s
	}

	s.query += " VALUES "
	for i := 0; i < l; i++ {
		s.buildColValues(colsValues[i])
		s.query += ", "
	}
	s.buildColValues(colsValues[l])

	return s
}

func (s *SqlQueryBuilder) Delete(tableName string) *SqlQueryBuilder {
	s.args = nil

	s.query = "DELETE FROM " + tableName

	return s
}

func (s *SqlQueryBuilder) Update(tableName string) *SqlQueryBuilder {
	s.args = nil

	s.query = "UPDATE " + tableName

	return s
}

func (s *SqlQueryBuilder) Set(updateFields map[string]interface{}) *SqlQueryBuilder {
	if updateFields == nil || len(updateFields) == 0 {
		return s
	}

	s.query += " SET "
	for name, value := range updateFields {
		s.query += name + " = ?, "
		s.args = append(s.args, value)
	}
	s.query = s.query[0 : len(s.query)-2]

	return s
}

func (s *SqlQueryBuilder) Select(what, tableName string) *SqlQueryBuilder {
	s.args = nil

	s.query = "SELECT " + what + " FROM " + tableName

	return s
}

func (s *SqlQueryBuilder) WhereConditionAnd(condItems ...*SqlColQueryItem) *SqlQueryBuilder {
	if len(condItems) == 0 {
		return s
	}

	s.query += " WHERE "

	s.buildWhereCondition("AND", condItems...)

	return s
}

func (s *SqlQueryBuilder) WhereConditionOr(condItems ...*SqlColQueryItem) *SqlQueryBuilder {
	if len(condItems) == 0 {
		return s
	}

	s.query += " WHERE "

	s.buildWhereCondition("OR", condItems...)

	return s
}

func (s *SqlQueryBuilder) OrderBy(orderBy string) *SqlQueryBuilder {
	if orderBy != "" {
		s.query += " ORDER BY " + orderBy
	}

	return s
}

func (s *SqlQueryBuilder) GroupBy(groupBy string) *SqlQueryBuilder {
	if groupBy != "" {
		s.query += " GROUP BY " + groupBy
	}

	return s
}

func (s *SqlQueryBuilder) HavingConditionAnd(condItems ...*SqlColQueryItem) *SqlQueryBuilder {
	if len(condItems) == 0 {
		return s
	}

	s.query += " HAVING "

	s.buildWhereCondition("AND", condItems...)

	return s
}

func (s *SqlQueryBuilder) HavingConditionOr(condItems ...*SqlColQueryItem) *SqlQueryBuilder {
	if len(condItems) == 0 {
		return s
	}

	s.query += " HAVING "

	s.buildWhereCondition("OR", condItems...)

	return s
}

func (s *SqlQueryBuilder) Limit(offset, cnt int64) *SqlQueryBuilder {
	if cnt <= 0 {
		return s
	}

	if offset < 0 {
		s.query += " LIMIT ?"
		s.args = append(s.args, cnt)

		return s
	}

	s.query += " LIMIT ?, ?"
	s.args = append(s.args, offset, cnt)

	return s
}

func (s *SqlQueryBuilder) buildColValues(colValues []interface{}) {
	l := len(colValues) - 1
	if l == -1 {
		return
	}

	s.query += "("

	for i := 0; i < l; i++ {
		s.query += "?, "
		s.args = append(s.args, colValues[i])
	}

	s.query += "?)"
	s.args = append(s.args, colValues[l])
}

func (s *SqlQueryBuilder) buildWhereCondition(andOr string, condItems ...*SqlColQueryItem) {
	l := len(condItems) - 1
	if l == -1 {
		return
	}

	for i := 0; i < l; i++ {
		s.buildCondition(condItems[i])
		s.query += " " + andOr + " "
	}
	s.buildCondition(condItems[l])
}

func (s *SqlQueryBuilder) buildCondition(condItem *SqlColQueryItem) {
	switch condItem.Condition {
	case SqlCondEqual, SqlCondNotEqual, SqlCondLess, SqlCondLessEqual, SqlCondGreater, SqlCondGreaterEqual:
		s.query += condItem.Name + " " + condItem.Condition + " ?"
		s.args = append(s.args, condItem.Value)
	case SqlCondIn:
		s.buildConditionInOrNotIn(condItem, "IN")
	case SqlCondNotIn:
		s.buildConditionInOrNotIn(condItem, "NOT IN")
	case SqlCondLike:
		s.query += condItem.Name + " LIKE ?"
		s.args = append(s.args, condItem.Value)
	case SqlCondBetween:
		rev := reflect.ValueOf(condItem.Value)
		s.query += condItem.Name + " BETWEEN ? AND ?"
		s.args = append(s.args, rev.Index(0).Interface(), rev.Index(1).Interface())
	}
}

func (s *SqlQueryBuilder) buildConditionInOrNotIn(condItem *SqlColQueryItem, inOrNotIn string) {
	rev := reflect.ValueOf(condItem.Value)
	l := rev.Len() - 1
	if l == -1 {
		return
	}

	s.query += condItem.Name + " " + inOrNotIn + " ("
	for i := 0; i < l; i++ {
		s.query += "?, "
	}
	s.query += "?)"

	for i := 0; i < rev.Len(); i++ {
		s.args = append(s.args, rev.Index(i).Interface())
	}
}
