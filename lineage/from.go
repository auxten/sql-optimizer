package lineage

import (
	"strings"

	"github.com/auxten/go-sql-lineage/utils"
	"github.com/auxten/postgresql-parser/pkg/sql/sem/tree"
	log "github.com/sirupsen/logrus"
)

type GetProvidedCtx struct {
	Err error
}

// GetInputColumns returns a list of output columns of the subqueries or tables.
// Every table will also have a ColExpr with Name == "" for get output tables easily.
func GetInputColumns(inputSchema Tables, subqueryOrTables ...interface{}) (columns ColExprs, tables TableExprs) {
	columns = make(ColExprs, 0, len(subqueryOrTables)+len(inputSchema))
	tables = make(TableExprs, 0, len(subqueryOrTables)+len(inputSchema))
	for i, subqueryOrTable := range subqueryOrTables {
		//log.Debugf("Table: %T\n\t%s", subqueryOrTable, subqueryOrTable)
		switch expr := subqueryOrTable.(type) {
		case tree.TableExprs:
			for _, tableExpr := range expr {
				cols, tabs := GetInputColumns(inputSchema, tableExpr)
				columns = append(columns, cols...)
				tables = append(tables, tabs...)
			}
		case *tree.AliasedTableExpr:
			tableAlias := expr.As.Alias.Normalize()
			if len(tableAlias) != 0 {
				if len(expr.As.Cols) != 0 {
					// AS name(col1, col2) output name.col1 and name.col2
					for _, col := range expr.As.Cols {
						stack := utils.NewStack(1)
						stack.Push(expr)
						columns = append(columns, ColExpr{
							Name:     col.Normalize(),
							SrcTable: []string{expr.As.Alias.Normalize()},
							Scope:    stack,
							ColType:  Unknown,
						})
					}
					tables = append(tables, TableExpr{
						Name:      expr.As.Alias.Normalize(),
						TableType: Logical,
					})
					return
				}
				// AS name, replace output table name with alias
				cols, tabs := GetInputColumns(inputSchema, expr.Expr)
				for _, col := range cols {
					columns = append(columns, ColExpr{
						Name:     strings.ToLower(col.Name),
						SrcTable: []string{strings.ToLower(tableAlias)},
						Scope:    col.Scope,
						ColType:  col.ColType,
					})
				}
				tabType := Logical
				aliasTo := ""
				if len(tabs) == 1 && tabs[0].TableType == Physical {
					tabType = Physical
					aliasTo = tabs[0].Name
				}
				tables = append(tables, TableExpr{
					Name:        tableAlias,
					AliasTo:     aliasTo,
					AliasClause: &subqueryOrTables[i].(*tree.AliasedTableExpr).As,
					TableType:   tabType,
				})

			} else {
				cols, tabs := GetInputColumns(inputSchema, expr.Expr)
				columns = append(columns, cols...)
				tables = append(tables, tabs...)
			}
		case *tree.TableName:
			if len(inputSchema) != 0 {
				columns = append(columns, GetPhysicalTableColumns(inputSchema, expr.Table())...)
			}
			tables = append(tables, TableExpr{
				Name:      expr.Table(),
				TableType: Physical,
			})
		case *tree.Subquery:
			cols, _ := GetInputColumns(inputSchema, expr.Select)
			columns = append(columns, cols...)
		// *tree.ParenSelect, *tree.UnionClause, *tree.SelectClause are the only 3 possible
		//implementations of tree.SelectStatement
		case *tree.ParenSelect:
			cols, _ := GetInputColumns(inputSchema, expr.Select.Select)
			columns = append(columns, cols...)
		case *tree.UnionClause:
			/*
				Union output columns is only relevant with left expr
			*/
			cols, _ := GetInputColumns(inputSchema, expr.Left.Select)
			columns = append(columns, cols...)
		case *tree.SelectClause:
			columns = append(columns, GetOutputColumns(inputSchema, expr)...)
		case *tree.JoinTableExpr:
			lCols, lTabs := GetInputColumns(inputSchema, expr.Left)
			columns = append(columns, lCols...)
			tables = append(tables, lTabs...)
			rCols, rTabs := GetInputColumns(inputSchema, expr.Right)
			columns = append(columns, rCols...)
			tables = append(tables, rTabs...)
		}
	}

	return
}

func GetOutputColumns(inputSchema Tables, queryExpr *tree.SelectClause) (columns ColExprs) {
	columns = make(ColExprs, 0, len(queryExpr.Exprs))
	for _, proj := range queryExpr.Exprs {
		var (
			colExpr               ColExpr
			projTable, projColumn string
			err                   error
		)

		if IsColumn(proj.Expr) {
			if projTable, projColumn, err = ResolveExprTableColumn(proj.Expr); err != nil {
				//TODO: better error reporting
				log.Panic(err)
			}
		} else {
			projColumn = proj.Expr.String()
		}

		projTable = strings.ToLower(projTable)
		projColumn = strings.ToLower(projColumn)
		colExpr.Name = projColumn
		if len(projTable) != 0 {
			colExpr.SrcTable = []string{projTable}
		}

		// if we find the column in `FROM` clause, then ExprType and Scope could be more deterministic.
		// so try to find it!
		columnFound := false
		tableMatch := false
	searchFrom:
		for _, table := range queryExpr.From.Tables {
			fromCols, _ := GetInputColumns(inputSchema, table)
			for _, fromCol := range fromCols {
				if len(projTable) != 0 {
					for _, src := range fromCol.SrcTable {
						if projTable == strings.ToLower(src) {
							tableMatch = true
							break
						}
					}
				}
				// table and column both match
				if len(projTable) == 0 || tableMatch {
					if projColumn == strings.ToLower(fromCol.Name) {
						columnFound = true
						colExpr.ColType = fromCol.ColType
						scope := utils.NewStack(1)
						scope.Push(table)
						colExpr.Scope = scope
						break searchFrom
					}
				}
			}
		}

		if !columnFound {
			colExpr.ColType = Unknown
		}
		//// Special case for `SELECT a, t1.b FROM t1`
		//if (len(projTable) == 0 || tableMatch) && !columnFound && len(queryExpr.From.Tables) == 1 {
		//
		//	colExpr.SrcTable
		//}

		// Subquery `(SELECT t1.a AS aa FROM t1)` output column names `aa`
		if len(proj.As) != 0 {
			colExpr.Name = proj.As.String()
		}
		columns = append(columns, colExpr)
	}
	return
}

func GetPhysicalTableColumns(inputSchema Tables, table string) (columns ColExprs) {
	for _, t := range inputSchema {
		normalTableName := strings.ToLower(table)
		if strings.ToLower(t.Name) == normalTableName {
			columns = make(ColExprs, len(t.Cols))
			for i, col := range t.Cols {
				columns[i] = ColExpr{
					Name:     strings.ToLower(col.Name),
					SrcTable: []string{normalTableName},
					Scope:    nil,
					ColType:  Physical,
				}
			}
		}
	}
	return
}
