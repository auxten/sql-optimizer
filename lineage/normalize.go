package lineage

import (
	"strings"

	"github.com/auxten/sql-optimizer/utils"
	"github.com/auxten/postgresql-parser/pkg/sql/sem/tree"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

type NormalizeCtx struct {
	table            string
	column           string
	normalTableName  string
	normalColumnName string
	nextAction       Action
	UserCtx          *UserCtx
	Walker           *AstWalker
	Err              error
}

type UserCtx struct {
	//TableAliasList is `[]*tree.AliasClause`, for removing table alias after normalization.
	TableAliasList *utils.Stack
	TableCols      ColExprs     // table.column referenced in SQL statement
	DeferredFuncs  *utils.Stack // stack of `func() error`
}

// NormalizeColumnName will try to get the deterministic table name and column name
// `expr` is searched in the `treeRange` with the input table schema `tables`.
func NormalizeColumnName(
	expr tree.Expr, treeRange interface{}, inputPhysicalSchema []Table, stack *utils.Stack, userCtx *UserCtx,
) (tableName string, columnName string, err error) {
	ctx := &NormalizeCtx{UserCtx: userCtx}
	var initialSearchScope *utils.Stack
	if stack != nil {
		initialSearchScope = stack.Copy(0, stack.Len())
		initialSearchScope.Push(treeRange)
	}
	w := &AstWalker{
		Ctx:       ctx,
		NodeStack: stack,
		Fn: func(context interface{}, node interface{}) (newNode interface{}, action Action) {
			c := context.(*NormalizeCtx)
			if c.nextAction == Stop {
				return nil, Stop
			}
			// Set normal table and column name to original one
			c.normalTableName = c.table
			c.normalColumnName = c.column
			if len(c.table) != 0 {
				// table name existence is not validated here, db engine will do that
				switch n := node.(type) {
				case *tree.AliasedTableExpr:
					if c.table == n.As.Alias.Normalize() {
						switch e := n.Expr.(type) {
						case *tree.TableName:
							//Fixme: column list is ignored
							if len(n.As.Cols) != 0 {
								c.Err = errors.Errorf("AS with column list not supported: %s", n)
								c.nextAction = Stop
								return nil, Stop
							}
							c.normalTableName = e.Table()
							c.normalColumnName = c.column

							c.UserCtx.DeferredFuncs.Push(func() func() error {
								exp := expr
								tblName := c.normalTableName
								return func() error {
									return SetExprTableName(exp, tblName)
								}
							}())

							//c.Err = SetExprTableName(expr.(tree.Expr), e.Table())
							//if c.Err != nil {
							//	return Stop
							//}

							/*
								[^tableAlias]: Table name alias will shadow the original name.
								So if you want to keep the SQL executable, after going through
								all the table references, you should set tha table.As.Alias = "".
							*/

							if c.UserCtx.TableAliasList != nil {
								c.UserCtx.TableAliasList.Push(&n.As)
							}
							if c.UserCtx.TableCols != nil {
								c.UserCtx.TableCols = append(c.UserCtx.TableCols, ColExpr{
									Name:     c.column,
									SrcTable: []string{c.normalTableName},
									Scope:    initialSearchScope,
									ColType:  Physical,
								})
							}

							// if table name is an alias, column name should be ok to stop normalizing
							c.nextAction = Stop
							return nil, Stop
						case *tree.Subquery:
							// subquery alias is ignored
							if c.UserCtx.TableCols != nil {
								c.UserCtx.TableCols = append(c.UserCtx.TableCols, ColExpr{
									Name:     c.column,
									SrcTable: []string{c.table},
									Scope:    initialSearchScope,
									ColType:  Logical,
								})
							}

							c.nextAction = Stop
							return nil, Stop
						}
					} else if len(n.As.Alias) == 0 {
						switch e := n.Expr.(type) {
						case *tree.TableName:
							if c.table == e.Table() {
								// just found the direct reference of table name
								if c.UserCtx.TableCols != nil {
									c.UserCtx.TableCols = append(c.UserCtx.TableCols, ColExpr{
										Name:     c.column,
										SrcTable: []string{c.table},
										Scope:    initialSearchScope,
										ColType:  Physical,
									})
								}
								c.nextAction = Stop
								return nil, Stop
							}
						}
					}
				}
			} else {
				// Search column without a table name
				switch n := node.(type) {
				case *tree.ParenSelect:
					// do not search into subquery, exprs inside subquery will be normalized later
					return nil, Stop
				case *tree.SelectExpr:
					AsStr := n.As.String()
					if c.column == AsStr {
						switch e := n.Expr.(type) {
						case *tree.FuncExpr, *tree.CoalesceExpr, *tree.CaseExpr:
							/*
								func call result as alias will not change the physical column reference
								so leave it alone. but we must try to normalize the column referenced in
								the func parameter.
							*/
							if c.UserCtx.TableCols != nil {
								c.UserCtx.TableCols = append(c.UserCtx.TableCols, ColExpr{
									Name:     c.column,
									SrcTable: []string{c.table},
									Scope:    initialSearchScope,
									ColType:  Logical,
								})
							}

							c.nextAction = Stop
							return nil, Stop
						case *tree.UnresolvedName:
							var (
								subRange     interface{}
								subTreeRange *tree.From
								newStack     *utils.Stack
							)
							subTreeRange, newStack = c.BackTraceSubQueryFromTables()
							if subTreeRange == nil {
								subRange = treeRange
							} else {
								subRange = subTreeRange
							}
							table, column, er := NormalizeColumnName(e, subRange, inputPhysicalSchema, newStack, c.UserCtx)
							if er != nil {
								c.Err = er
								c.nextAction = Stop
								return nil, Stop
							}

							c.UserCtx.DeferredFuncs.Push(func() func() error {
								exp := expr.(*tree.UnresolvedName)
								tblName := table
								colName := column
								return func() error {
									if len(tblName) > 0 {
										//exp.NumParts = 2
										//exp.Parts[1] = tblName
										if er := SetExprTableName(exp, tblName); er != nil {
											return er
										}
									} else {
										exp.NumParts = 1
									}
									exp.Parts[0] = colName
									return nil
								}
							}())

							if c.UserCtx.TableCols != nil {
								if len(table) == 0 {
									c.UserCtx.TableCols = append(c.UserCtx.TableCols, ColExpr{
										Name:     column,
										SrcTable: []string{},
										Scope:    initialSearchScope,
										ColType:  Unknown,
									})
								} else {
									c.UserCtx.TableCols = append(c.UserCtx.TableCols, ColExpr{
										Name:     column,
										SrcTable: []string{table},
										Scope:    initialSearchScope,
										ColType:  Physical,
									})
								}
							}

							if len(table) > 0 {
								c.normalTableName = table
							}
							c.normalColumnName = column
							c.nextAction = Stop
							return nil, Stop
						}
					} else if len(AsStr) == 0 || AsStr == `""` {
						// no alias, check if column name match
						if IsColumn(n.Expr) {
							if table, column, err := ResolveExprTableColumn(n.Expr); err != nil {
								c.Err = err
								c.nextAction = Stop
								return nil, Stop
							} else if c.column == column {
								/*
									Something like `SELECT t1.a FROM xxx WHERE a = 0;` where `xxx` can be a list of
									simple Table or JOIN/UNION.
									PS: Subquery must have an alias, so it's impossible here to be a simple Subquery.
									We find `a` in WHERE Clause is also referenced in SELECT projection,
								*/

								if len(table) != 0 {
									c.UserCtx.DeferredFuncs.Push(func() func() error {
										exp := expr
										tblName := table
										return func() error {
											return SetExprTableName(exp, tblName)
										}
									}())

									//c.Err = SetExprTableName(expr, table)
									//if c.Err != nil {
									//	c.nextAction = Stop
									//	return Stop
									//}
									c.UserCtx.TableCols = append(c.UserCtx.TableCols, ColExpr{
										Name:     column,
										SrcTable: []string{table},
										Scope:    initialSearchScope,
										ColType:  Unknown,
									})

									c.normalTableName = table
									c.normalColumnName = column
									c.nextAction = Stop
									return nil, Stop
								}
							}
						}
					}

				case *tree.From:
					// if a column without table name can not find any match on the whole AST
					if colExpr, err := ResolveExprFromTables(c, inputPhysicalSchema, n, expr); err != nil {
						c.Err = err
						c.nextAction = Stop
						return nil, Stop
					} else {
						if len(colExpr.SrcTable) == 1 && len(colExpr.SrcTable[0]) > 0 {
							c.UserCtx.DeferredFuncs.Push(func() func() error {
								exp := expr
								tblName := colExpr.SrcTable[0]
								return func() error {
									return SetExprTableName(exp, tblName)
								}
							}())

							//c.Err = SetExprTableName(expr, colExpr.SrcTable[0])
							//if c.Err != nil {
							//	c.nextAction = Stop
							//	return Stop
							//}
							c.normalTableName = colExpr.SrcTable[0]
							c.normalColumnName = colExpr.Name
							c.nextAction = Stop
							return nil, Stop
						}

						if c.UserCtx.TableCols != nil {
							c.UserCtx.TableCols = append(c.UserCtx.TableCols, *colExpr)
						}
						return nil, Stop
					}
				}
			}
			return nil, Goon
		},
	}
	// make Walker point to the ctx father
	ctx.Walker = w

	if ctx.table, ctx.column, err = ResolveExprTableColumn(expr); err != nil {
		return
	}

	w.WalkAST(treeRange)
	if ctx.Err != nil {
		err = ctx.Err
		return
	}

	return ctx.normalTableName, ctx.normalColumnName, nil
}

func (ctx *NormalizeCtx) BackTraceSubQueryFromTables() (subRange *tree.From, newStack *utils.Stack) {
	// trace back to the first From statement
	var subRangeSelect interface{}
	subRangeSelect, newStack = ctx.BackTraceSubQueryNewStack()

	// From is not a pointer in struct, so we must get From.Tables which is important for ast walker
	switch n := subRangeSelect.(type) {
	case *tree.SelectClause:
		subRange = &n.From
		newStack.Push(n)
	case *tree.Select:
		subRange = &n.Select.(*tree.SelectClause).From
		newStack.Push(n)
		newStack.Push(n.Select)
	default:
		subRange = nil
	}
	return
}

func (ctx *NormalizeCtx) BackTraceSubQueryNewStack() (subRange interface{}, newStack *utils.Stack) {
	// trace back to the first SelectClause statement
loop:
	for i := ctx.Walker.NodeStack.Len(); i > 0; i-- {
		if sn, ok := ctx.Walker.NodeStack.Get(i - 1); ok {
			switch sc := sn.(type) {
			case *tree.SelectClause, *tree.Select:
				newStack = ctx.Walker.NodeStack.Copy(0, i-1)
				subRange = sc
				break loop
			}
		} else {
			break
		}
	}
	return
}

func ResolveExprTableColumn(expr tree.Expr) (table string, column string, err error) {
	if !IsColumn(expr) {
		err = errors.Errorf("%s is not a column", expr)
		return
	}
	fullColName := expr.String()
	if fullColName == "*" {
		err = errors.Errorf("star(*) is not allowed, %s", expr)
		return
	}
	tableCol := strings.Split(fullColName, ".")
	switch len(tableCol) {
	case 0:
		err = errors.Errorf("empty column name: %v", expr)
		return
	case 1:
		// single column
		column = strings.ToLower(tableCol[0])
	case 2:
		// table.col is ok, but we need try to normalize tableAlias.col
		table = strings.ToLower(tableCol[0])
		column = strings.ToLower(tableCol[1])
	case 3, 4:
		// db.table.col, schema.db.table.col style name normalization is not necessary
		err = errors.Errorf("illegal column name: %s", fullColName)
		return
	}
	return
}

func NormalizeAST(ast tree.Statement, inputSchema Tables) (retAst tree.Statement, inputCols ColExprs, err error) {
	stack := utils.NewStack(0)
	userCtx := &UserCtx{
		TableAliasList: stack,
		TableCols:      make(ColExprs, 0),
		DeferredFuncs:  utils.NewStack(0),
	}

	ctx := &NormalizeCtx{UserCtx: userCtx}
	w := &AstWalker{
		Ctx: ctx,
		Fn: func(context interface{}, node interface{}) (_ interface{}, action Action) {
			ctx := context.(*NormalizeCtx)
			if IsColumn(node) {
				subRange, newStack := ctx.BackTraceSubQueryNewStack()
				lenBefore := len(ctx.UserCtx.TableCols)
				if _, _, err := NormalizeColumnName(node.(tree.Expr), subRange, inputSchema, newStack, ctx.UserCtx); err != nil {
					ctx.Err = err
					ctx.nextAction = Stop
					return nil, Stop
				}
				lenAfter := len(ctx.UserCtx.TableCols)
				if lenBefore == lenAfter {
					// TODO: the node expr can not be located in the subRange, try to find the possible SrcTable
					fromClause, _ := ctx.BackTraceSubQueryFromTables()
					var colExpr *ColExpr
					if colExpr, ctx.Err = ResolveExprFromTables(ctx, inputSchema, fromClause, node); ctx.Err != nil {
						ctx.nextAction = Stop
						return nil, Stop
					}
					if ctx.UserCtx.TableCols != nil {
						ctx.UserCtx.TableCols = append(ctx.UserCtx.TableCols, *colExpr)
					}
					if len(colExpr.SrcTable) > 0 && len(colExpr.SrcTable[0]) > 0 {
						ctx.UserCtx.DeferredFuncs.Push(func() func() error {
							exp := node.(tree.Expr)
							tblName := colExpr.SrcTable[0]
							return func() error {
								return SetExprTableName(exp, tblName)
							}
						}())
						//ctx.Err = SetExprTableName(node.(tree.Expr), colExpr.SrcTable[0])
						//if ctx.Err != nil {
						//	return Stop
						//}
					}
					return nil, Goon
				}
			}
			return nil, Goon
		},
	}
	// make Walker point to the ctx father
	ctx.Walker = w

	w.WalkAST(ast)
	if ctx.Err != nil {
		err = ctx.Err
		return
	}

	for {
		if f, ok := ctx.UserCtx.DeferredFuncs.Pop(); ok {
			if err = f.(func() error)(); err != nil {
				return
			}
		} else {
			break
		}
	}

	// Remove all the table alias. See [^tableAlias]
	for {
		if alias, ok := ctx.UserCtx.TableAliasList.Pop(); ok {
			alias.(*tree.AliasClause).Alias = ""
		} else {
			break
		}
	}
	return ast, ctx.UserCtx.TableCols, nil
}

func ResolveExprFromTables(ctx *NormalizeCtx, inputSchema []Table, from *tree.From, node interface{}) (
	colExpr *ColExpr, err error,
) {

	var (
		table, column string
		colType       = Unknown
		stack         = utils.NewStack(1)
		srcTable      string
		fromTables    = from.Tables
	)

	if table, column, err = ResolveExprTableColumn(node.(tree.Expr)); err != nil {
		return
	}
	outputCols, outputTabs := GetInputColumns(inputSchema, fromTables)
	for _, outputCol := range outputCols {
		if column == strings.ToLower(outputCol.Name) {
			tableMatch := false
			if len(table) != 0 {
				for _, outputTable := range outputCol.SrcTable {
					if table == strings.ToLower(outputTable) {
						tableMatch = true
						break
					}
				}
			}

			if tableMatch || len(table) == 0 { // src table is determined
				if len(table) == 0 && len(outputCol.SrcTable) == 1 {
					table = outputCol.SrcTable[0]
				}
				stack.Push(fromTables)
				colExpr = &ColExpr{
					Name:     column,
					SrcTable: []string{table},
					Scope:    stack,
					ColType:  outputCol.ColType,
				}
				return
			}
		}
	}

	tableFound := false

	if len(table) != 0 {
		// Try to find the table by its name.
		for _, outputTab := range outputTabs {
			if table == outputTab.Name {
				tableFound = true
				colType = outputTab.TableType
				if len(outputTab.AliasTo) != 0 {
					srcTable = outputTab.AliasTo
				} else {
					srcTable = outputTab.Name
					if ctx.UserCtx.TableAliasList != nil && colType == Physical && outputTab.AliasClause != nil {
						ctx.UserCtx.TableAliasList.Push(outputTab.AliasClause)
					}
				}
				break
			}
		}
		//findTableByName:
		//	for _, fromTable := range fromTables {
		//		switch fromTable := fromTable.(type) {
		//		case *tree.TableName:
		//			if table == fromTable.Table() {
		//				tableFound = true
		//				srcTable = table
		//				colType = Physical
		//				break findTableByName
		//			}
		//		case *tree.AliasedTableExpr:
		//			if table == fromTable.As.Alias.Normalize() {
		//				// If table alias column list is not empty, the GetInputColumns
		//				//should return the columns we can ignore this situation here.
		//				tableFound = true
		//				switch tab := fromTable.Expr.(type) {
		//				case *tree.TableName:
		//					srcTable = tab.Table()
		//					if ctx.UserCtx.TableAliasList != nil {
		//						ctx.UserCtx.TableAliasList.Push(&fromTable.As)
		//					}
		//					colType = Physical
		//				default:
		//					colType = Logical
		//				}
		//
		//				break findTableByName
		//			}
		//		}
		//	}
	} else if len(fromTables) == 1 {
		tableFound = true
		switch fromTable := fromTables[0].(type) {
		case *tree.TableName:
			srcTable = fromTable.Table()
			colType = Physical
		case *tree.AliasedTableExpr:
			switch tab := fromTable.Expr.(type) {
			case *tree.TableName:
				srcTable = tab.Table()
				if ctx.UserCtx.TableAliasList != nil {
					ctx.UserCtx.TableAliasList.Push(&fromTable.As)
				}
				colType = Physical
			default:
				colType = Logical
			}
		//case *tree.ParenTableExpr, *tree.JoinTableExpr, *tree.Subquery:
		default:
			colType = Logical
		}
	}

	srcTables := make([]string, 0, 1)
	if !tableFound {
		log.Debugf(
			"something is wrong searching %s in the SQL: FROM\n%s",
			node, tree.Pretty(&fromTables))
		colType = Unknown
	} else {
		srcTables = append(srcTables, srcTable)
	}

	stack.Push(fromTables)

	colExpr = &ColExpr{
		Name:     column,
		SrcTable: srcTables,
		Scope:    stack,
		ColType:  colType,
	}
	return

}

func SetExprTableName(expr tree.Expr, tableName string) error {
	//Fixme: expr type other than UnresolvedName is not processed
	if node, ok := expr.(*tree.UnresolvedName); ok {
		node.NumParts = 2
		node.Parts[1] = tableName
	} else {
		return errors.Errorf("unexpected expr type %t %s", expr, expr)
	}

	return nil
}
