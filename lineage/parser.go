package lineage

import (
	"github.com/auxten/postgresql-parser/pkg/sql/parser"
)

func sql2ast(sql string) (parser.Statements, error) {
	return parser.Parse(sql)
}

func AllColsContained(set ReferredCols, cols []string) bool {
	if cols == nil {
		if set == nil {
			return true
		} else {
			return false
		}
	}
	if len(set) != len(cols) {
		return false
	}
	for _, col := range cols {
		if _, exist := set[col]; !exist {
			return false
		}
	}
	return true
}

//GetTabColsMap gets physical table columns referred in Query and
//returns a map[table][column]isPhysicalColumn
func GetTabColsMap(inputCols ColExprs, logicalColIncluded bool, unresolvedColIncluded bool) map[string]map[string]bool {
	tabColsMap := make(map[string]map[string]bool)
	var (
		tab, col string
	)
	for _, c := range inputCols {
		if c.ColType == Physical {
			tab = c.SrcTable[0]
			col = c.Name
		} else if logicalColIncluded && c.ColType == Logical {
			tab = c.SrcTable[0]
			col = c.Name
		} else if unresolvedColIncluded && c.ColType == Unknown && len(c.SrcTable) == 0 {
			tab = ""
			col = c.Name
		} else {
			continue
		}

		if _, ok := tabColsMap[tab]; !ok {
			tabColsMap[tab] = map[string]bool{col: c.ColType == Physical}
		} else {
			tabColsMap[tab][col] = c.ColType == Physical
		}
	}

	return tabColsMap
}
