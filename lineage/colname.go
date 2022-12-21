package lineage

import (
	"fmt"
)

type ColumnName string

func (c ColumnName) TabName() string {
	hasDot := false
	i := 0
	for ; i < len(c); i++ {
		if c[i] == '.' {
			hasDot = true
			break
		}
	}
	if !hasDot {
		return ""
	}
	return string(c[:i])
}

func (c ColumnName) ColName() string {
	hasDot := false
	i := 0
	for ; i < len(c); i++ {
		if c[i] == '.' {
			hasDot = true
			break
		}
	}
	if !hasDot {
		return string(c)
	}
	return string(c[i+1:])
}

func NewColumnName(table string, column string) ColumnName {
	return ColumnName(fmt.Sprintf("%s.%s", table, column))
}
