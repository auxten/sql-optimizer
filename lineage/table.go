package lineage

import (
	"fmt"
	"strings"

	"github.com/auxten/go-sql-lineage/utils"
	"github.com/auxten/postgresql-parser/pkg/sql/sem/tree"
	"github.com/cockroachdb/errors"
)

type TableExpr struct {
	Name        string
	AliasTo     string
	AliasClause *tree.AliasClause
	TableType   ExprType
}

type TableExprs []TableExpr

//ColExpr is a source identified column expression.
//TODO: introduce a ColExpr interface type, maybe.
type ColExpr struct {
	Name     string
	SrcTable []string // length == 1 means source table is deterministic, else all elements are possible.
	Scope    *utils.Stack
	// ColType is the type of column
	// Physical column is a column in input physical table, otherwise it can be
	// *tree.FuncExpr, *tree.CoalesceExpr, *tree.CaseExpr
	ColType ExprType
}

type ColExprs []ColExpr

type ExprType int8

const (
	Unknown ExprType = iota
	Physical
	Logical
)

func (t ExprType) String() string {
	return [...]string{"Unknown", "Physical", "Logical"}[t]
}

func (t TableExpr) String() string {
	return t.Name
}

func (ts TableExprs) GetTable(tblName string) (*TableExpr, error) {
	if len(tblName) == 0 {
		return nil, errors.Errorf("invalid table name: %v", tblName)
	}
	tblName = strings.ToLower(tblName)
	for i := range ts {
		if ts[i].Name == tblName || ts[i].AliasTo == tblName {
			return &ts[i], nil
		}
	}
	return nil, errors.New("table not found")
}

func (c ColExpr) ExprName() string {
	switch len(c.SrcTable) {
	case 0:
		return fmt.Sprint(c.Name)
	case 1:
		if len(c.SrcTable[0]) == 0 {
			return fmt.Sprint(c.Name)
		} else {
			return fmt.Sprintf("%s.%s", c.SrcTable[0], c.Name)
		}
	default:
		return fmt.Sprintf("[%s].%s", c.SrcTable, c.Name)
	}
}

func (c ColExpr) String() string {
	switch len(c.SrcTable) {
	case 0:
		return fmt.Sprintf("%s:%s", c.Name, c.ColType)
	case 1:
		if len(c.SrcTable[0]) == 0 {
			return fmt.Sprintf("%s:%s", c.Name, c.ColType)
		} else {
			return fmt.Sprintf("%s.%s:%s", c.SrcTable[0], c.Name, c.ColType)
		}
	default:
		return fmt.Sprintf("[%s].%s:%s", c.SrcTable, c.Name, c.ColType)
	}
}

func (c ColExpr) DebugString() string {
	var stackStr string
	stackStr = "\n"
	previousSQL := "impossibleSQL"
	if c.Scope != nil {
		for i := c.Scope.Len(); i > 0; i-- {
			if sn, ok := c.Scope.Get(i - 1); ok {
				currentSQL := fmt.Sprint(sn)
				replaced := strings.ReplaceAll(currentSQL, previousSQL, "{$PREV}")
				stackStr += fmt.Sprintf("%s [%T] %s\n", strings.Repeat("\t", c.Scope.Len()-i), sn, replaced)
				previousSQL = currentSQL
			}
		}
	}

	switch len(c.SrcTable) {
	case 0:
		return fmt.Sprintf("%s %s @%+v\n", c.Name, c.ColType, stackStr)
	case 1:
		if len(c.SrcTable[0]) == 0 {
			return fmt.Sprintf("%s %s @%+v\n", c.Name, c.ColType, stackStr)
		} else {
			return fmt.Sprintf("%s.%s %s @%+v\n", c.SrcTable[0], c.Name, c.ColType, stackStr)
		}
	default:
		return fmt.Sprintf("[%s].%s %s @%+v\n", c.SrcTable, c.Name, c.ColType, stackStr)
	}
}

func (l ColExprs) ToList() []string {
	ret := make([]string, len(l))
	for i, c := range l {
		switch len(c.SrcTable) {
		case 0:
			ret[i] = fmt.Sprint(c.Name)
		case 1:
			if len(c.SrcTable[0]) == 0 {
				ret[i] = fmt.Sprint(c.Name)
			} else {
				ret[i] = fmt.Sprintf("%s.%s", c.SrcTable[0], c.Name)
			}
		default:
			ret[i] = fmt.Sprintf("[%s].%s", c.SrcTable, c.Name)
		}
	}

	return utils.SortDeDup(ret)
}
