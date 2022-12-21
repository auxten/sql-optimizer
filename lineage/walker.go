package lineage

import (
	"fmt"
	"strings"

	"github.com/auxten/go-sql-lineage/utils"
	"github.com/auxten/postgresql-parser/pkg/sql/parser"
	"github.com/auxten/postgresql-parser/pkg/sql/sem/tree"
	log "github.com/sirupsen/logrus"
)

type Action int8

const (
	// Goon go on as everything is ok.
	Goon Action = iota
	// Stop means return current WalkAST function, stop walking deeper. see NormalizeCtx.nextAction
	// TODO: move stop logic into WalkAST function.
	Stop
	// Continue means stop walking deeper, continue to next node
	Continue
)

type AstWalker struct {
	NodeCount    []int
	UnknownNodes []interface{}
	NodeStack    *utils.Stack
	Ctx          interface{}
	// Fn the function to be called when the walk.
	//when newNode is not nil and action is Continue, AstWalker.Walk will stop walk deeper and return which will cause
	//the AST node replaced by the newNode.
	Fn func(ctx interface{}, node interface{}) (newNode interface{}, action Action)
}

type ReferredCols map[string]int

func (rc ReferredCols) ToList() []string {
	cols := make([]string, len(rc))
	i := 0
	for k := range rc {
		cols[i] = k
		i++
	}

	return utils.SortDeDup(cols)
}

func (w *AstWalker) Walk(stmts parser.Statements) (ok bool, err error) {
	asts := make([]tree.NodeFormatter, len(stmts))
	for si, stmt := range stmts {
		asts[si] = stmt.AST
	}

	// nodeCount is incremented on each visited node per statement. It is
	// currently used to determine if walk is at the top-level statement
	// or not.
	for _, ast := range asts {
		w.WalkAST(ast)
	}

	return true, nil
}

type nodeRef interface {
	tree.NodeFormatter
}

type nodeRefs []nodeRef

func (w *AstWalker) WalkAST(ni interface{}) (newNode interface{}) {
	var (
		retAction Action
	)

	if w.NodeCount == nil {
		w.NodeCount = make([]int, 1)
	}
	w.NodeCount[0]++
	if w.Fn != nil {
		newNode, retAction = w.Fn(w.Ctx, ni)
		if retAction == Goon {
			// go on as usual.
		} else if retAction == Stop {
			return nil
		} else if retAction == Continue {
			return newNode
		}
	}

	if w.NodeStack == nil {
		w.NodeStack = utils.NewStack(4)
	}
	w.NodeStack.Push(ni)
	defer w.NodeStack.Pop()

	switch node := ni.(type) {
	case *tree.AliasedTableExpr:
		if nn := w.WalkAST(node.Expr); nn != nil {
			node.Expr = nn.(tree.TableExpr)
		}
	case *tree.AndExpr:
		if nn := w.WalkAST(node.Left); nn != nil {
			node.Left = nn.(tree.Expr)
		}
		if nn := w.WalkAST(node.Right); nn != nil {
			node.Right = nn.(tree.Expr)
		}
	case *tree.AnnotateTypeExpr:
		if nn := w.WalkAST(node.Expr); nn != nil {
			node.Expr = nn.(tree.Expr)
		}

	case *tree.Array:
		if nn := w.WalkAST(node.Exprs); nn != nil {
			node.Exprs = nn.(tree.Exprs)
		}
	case *tree.AsOfClause:
		if nn := w.WalkAST(node.Expr); nn != nil {
			node.Expr = nn.(tree.Expr)
		}
	case *tree.BinaryExpr:
		if nn := w.WalkAST(node.Left); nn != nil {
			node.Left = nn.(tree.Expr)
		}
		if nn := w.WalkAST(node.Right); nn != nil {
			node.Right = nn.(tree.Expr)
		}
	case *tree.CaseExpr:
		if nn := w.WalkAST(node.Expr); nn != nil {
			node.Expr = nn.(tree.Expr)
		}
		if nn := w.WalkAST(node.Else); nn != nil {
			node.Else = nn.(tree.Expr)
		}
		if nn := w.WalkAST(node.Whens); nn != nil {
			node.Whens = nn.([]*tree.When)
		}
	case *tree.RangeCond:
		if nn := w.WalkAST(node.Left); nn != nil {
			node.Left = nn.(tree.Expr)
		}
		if nn := w.WalkAST(node.From); nn != nil {
			node.From = nn.(tree.Expr)
		}
		if nn := w.WalkAST(node.To); nn != nil {
			node.To = nn.(tree.Expr)
		}
	case *tree.CastExpr:
		if nn := w.WalkAST(node.Expr); nn != nil {
			node.Expr = nn.(tree.Expr)
		}
	case *tree.CoalesceExpr:
		if nn := w.WalkAST(node.Exprs); nn != nil {
			node.Exprs = nn.(tree.Exprs)
		}
	case *tree.ColumnTableDef:
	case *tree.ComparisonExpr:
		if nn := w.WalkAST(node.Left); nn != nil {
			node.Left = nn.(tree.Expr)
		}
		if nn := w.WalkAST(node.Right); nn != nil {
			node.Right = nn.(tree.Expr)
		}
	case *tree.CreateTable:
		if nn := w.WalkAST(node.Defs); nn != nil {
			node.Defs = nn.(tree.TableDefs)
		}
		if nn := w.WalkAST(node.AsSource); nn != nil {
			node.AsSource = nn.(*tree.Select)
		}
	case *tree.CTE:
		if nn := w.WalkAST(node.Stmt); nn != nil {
			node.Stmt = nn.(tree.Statement)
		}
	case []*tree.CTE:
		for i := range node {
			if nn := w.WalkAST(node[i]); nn != nil {
				node[i] = nn.(*tree.CTE)
			}
		}
	case tree.DistinctOn:
		for i := range node {
			if nn := w.WalkAST(node[i]); nn != nil {
				node[i] = nn.(tree.Expr)
			}
		}
	case *tree.DBool:
	case tree.Exprs:
		for i := range node {
			if nn := w.WalkAST(node[i]); nn != nil {
				node[i] = nn.(tree.Expr)
			}
		}
	case []tree.Exprs:
		for i := range node {
			if nn := w.WalkAST(node[i]); nn != nil {
				node[i] = nn.(tree.Exprs)
			}
		}
	case *tree.FamilyTableDef:
	case *tree.From:
		if nn := w.WalkAST(node.AsOf); nn != nil {
			node.AsOf = nn.(tree.AsOfClause)
		}
		if nn := w.WalkAST(node.Tables); nn != nil {
			node.Tables = nn.(tree.TableExprs)
		}
	case *tree.FuncExpr:
		if node.WindowDef != nil {
			if nn := w.WalkAST(node.WindowDef); nn != nil {
				node.WindowDef = nn.(*tree.WindowDef)
			}
		}
		if nn := w.WalkAST(node.Exprs); nn != nil {
			node.Exprs = nn.(tree.Exprs)
		}
		if nn := w.WalkAST(node.Filter); nn != nil {
			node.Filter = nn.(tree.Expr)
		}
	case tree.GroupBy:
		for i := range node {
			if nn := w.WalkAST(node[i]); nn != nil {
				node[i] = nn.(tree.Expr)
			}
		}
	case *tree.IndexTableDef:
	case *tree.JoinTableExpr:
		if nn := w.WalkAST(node.Left); nn != nil {
			node.Left = nn.(tree.TableExpr)
		}
		if nn := w.WalkAST(node.Right); nn != nil {
			node.Right = nn.(tree.TableExpr)
		}
		if nn := w.WalkAST(node.Cond); nn != nil {
			node.Cond = nn.(tree.JoinCond)
		}
	case *tree.Limit:
		if nn := w.WalkAST(node.Offset); nn != nil {
			node.Offset = nn.(tree.Expr)
		}
		if nn := w.WalkAST(node.Count); nn != nil {
			node.Count = nn.(tree.Expr)
		}
	case *tree.NotExpr:
		if nn := w.WalkAST(node.Expr); nn != nil {
			node.Expr = nn.(tree.Expr)
		}
	case *tree.NumVal:
	case *tree.OnJoinCond:
		if nn := w.WalkAST(node.Expr); nn != nil {
			node.Expr = nn.(tree.Expr)
		}
	case *tree.Order:
		if nn := w.WalkAST(node.Expr); nn != nil {
			node.Expr = nn.(tree.Expr)
		}
		if nn := w.WalkAST(node.Table); nn != nil {
			node.Table = nn.(tree.TableName)
		}
	case tree.OrderBy:
		for i := range node {
			if nn := w.WalkAST(node[i]); nn != nil {
				node[i] = nn.(*tree.Order)
			}
		}
	case *tree.OrExpr:
		if nn := w.WalkAST(node.Left); nn != nil {
			node.Left = nn.(tree.Expr)
		}
		if nn := w.WalkAST(node.Right); nn != nil {
			node.Right = nn.(tree.Expr)
		}
	case *tree.ParenExpr:
		if nn := w.WalkAST(node.Expr); nn != nil {
			node.Expr = nn.(tree.Expr)
		}
	case *tree.ParenSelect:
		if nn := w.WalkAST(node.Select); nn != nil {
			node.Select = nn.(*tree.Select)
		}
	case *tree.RowsFromExpr:
		if nn := w.WalkAST(node.Items); nn != nil {
			node.Items = nn.(tree.Exprs)
		}
	case *tree.Select:
		if node.With != nil {
			if nn := w.WalkAST(node.With); nn != nil {
				node.With = nn.(*tree.With)
			}
		}
		if node.OrderBy != nil {
			if nn := w.WalkAST(node.OrderBy); nn != nil {
				node.OrderBy = nn.(tree.OrderBy)
			}
		}
		if node.Limit != nil {
			if nn := w.WalkAST(node.Limit); nn != nil {
				node.Limit = nn.(*tree.Limit)
			}
		}
		if nn := w.WalkAST(node.Select); nn != nil {
			node.Select = nn.(tree.SelectStatement)
		}
	case *tree.SelectClause:
		if nn := w.WalkAST(node.Exprs); nn != nil {
			node.Exprs = nn.(tree.SelectExprs)
		}
		if node.Where != nil {
			if nn := w.WalkAST(node.Where); nn != nil {
				node.Where = nn.(*tree.Where)
			}
		}
		if node.Having != nil {
			if nn := w.WalkAST(node.Having); nn != nil {
				node.Having = nn.(*tree.Where)
			}
		}
		if node.DistinctOn != nil {
			if nn := w.WalkAST(node.DistinctOn); nn != nil {
				node.DistinctOn = nn.(tree.DistinctOn)
			}
		}
		if node.GroupBy != nil {
			if nn := w.WalkAST(node.GroupBy); nn != nil {
				node.GroupBy = nn.(tree.GroupBy)
			}
		}
		if nn := w.WalkAST(&node.From); nn != nil {
			node.From = nn.(tree.From)
		}
	case *tree.SelectExpr:
		if nn := w.WalkAST(node.Expr); nn != nil {
			node.Expr = nn.(tree.Expr)
		}
	case tree.SelectExprs:
		for i := range node {
			if nn := w.WalkAST(&node[i]); nn != nil {
				node[i] = nn.(tree.SelectExpr)
			}
		}
	case *tree.SetVar:
		if nn := w.WalkAST(node.Values); nn != nil {
			node.Values = nn.(tree.Exprs)
		}
	case *tree.StrVal:
	case *tree.Subquery:
		if nn := w.WalkAST(node.Select); nn != nil {
			node.Select = nn.(tree.SelectStatement)
		}
	case tree.TableDefs:
		for i := range node {
			if nn := w.WalkAST(node[i]); nn != nil {
				node[i] = nn.(tree.TableDef)
			}
		}
	case tree.TableExprs:
		for i := range node {
			if nn := w.WalkAST(node[i]); nn != nil {
				node[i] = nn.(tree.TableExpr)
			}
		}
	case *tree.TableName, tree.TableName:
	case *tree.Tuple:
		if nn := w.WalkAST(node.Exprs); nn != nil {
			node.Exprs = nn.(tree.Exprs)
		}
	case *tree.UnaryExpr:
		if nn := w.WalkAST(node.Expr); nn != nil {
			node.Expr = nn.(tree.Expr)
		}
	case *tree.UniqueConstraintTableDef:
	case *tree.UnionClause:
		if nn := w.WalkAST(node.Left); nn != nil {
			node.Left = nn.(*tree.Select)
		}
		if nn := w.WalkAST(node.Right); nn != nil {
			node.Right = nn.(*tree.Select)
		}
	case tree.UnqualifiedStar:
	case *tree.UnresolvedName:
	case *tree.ValuesClause:
		if nn := w.WalkAST(node.Rows); nn != nil {
			node.Rows = nn.([]tree.Exprs)
		}
	case *tree.When:
		if nn := w.WalkAST(node.Val); nn != nil {
			node.Val = nn.(tree.Expr)
		}
		if nn := w.WalkAST(node.Cond); nn != nil {
			node.Cond = nn.(tree.Expr)
		}
	case []*tree.When:
		for i := range node {
			if nn := w.WalkAST(node[i]); nn != nil {
				node[i] = nn.(*tree.When)
			}
		}
	case *tree.Where:
		if nn := w.WalkAST(node.Expr); nn != nil {
			node.Expr = nn.(tree.Expr)
		}
	case tree.Window:
		for i := range node {
			if nn := w.WalkAST(node[i]); nn != nil {
				node[i] = nn.(*tree.WindowDef)
			}
		}
	case *tree.WindowDef:
		if nn := w.WalkAST(node.Partitions); nn != nil {
			node.Partitions = nn.(tree.Exprs)
		}
		if node.Frame != nil {
			if nn := w.WalkAST(node.Frame); nn != nil {
				node.Frame = nn.(*tree.WindowFrame)
			}
		}
	case *tree.WindowFrame:
		if nn := w.WalkAST(&node.Bounds); nn != nil {
			node.Bounds = nn.(tree.WindowFrameBounds)
		}
	case *tree.WindowFrameBounds:
		if node.StartBound != nil {
			if nn := w.WalkAST(node.StartBound); nn != nil {
				node.StartBound = nn.(*tree.WindowFrameBound)
			}
		}
		if node.EndBound != nil {
			if nn := w.WalkAST(node.EndBound); nn != nil {
				node.EndBound = nn.(*tree.WindowFrameBound)
			}
		}
	case *tree.WindowFrameBound:
		if nn := w.WalkAST(node.OffsetExpr); nn != nil {
			node.OffsetExpr = nn.(tree.Expr)
		}
	case *tree.With:
		if nn := w.WalkAST(node.CTEList); nn != nil {
			node.CTEList = nn.([]*tree.CTE)
		}
	default:
		if w.UnknownNodes == nil {
			w.UnknownNodes = make([]interface{}, 0)
		}
		w.UnknownNodes = append(w.UnknownNodes, node)
	}
	return
}

func IsColumn(node interface{}) bool {
	switch node.(type) {
	// it's wired that the "Subquery" type is also "VariableExpr" type
	// we have to ignore that case.
	case *tree.Subquery:
		return false
	case tree.VariableExpr:
		return true
	}
	return false
}

// ColNamesInSelect finds all referred variables in a Select Statement.
// (variables = sub-expressions, placeholders, indexed vars, etc.)
// Implementation limits:
//	1. Table with AS is not normalized.
//  2. Columns referred from outer query are not translated.
func ColNamesInSelect(sql string) (referredCols ReferredCols, err error) {
	referredCols = make(ReferredCols)

	w := &AstWalker{
		Ctx: referredCols,
		Fn: func(ctx interface{}, node interface{}) (_ interface{}, action Action) {
			rCols := ctx.(ReferredCols)
			if IsColumn(node) {
				nodeName := fmt.Sprint(node)
				// just drop the "table." part
				tableCols := strings.Split(nodeName, ".")
				colName := tableCols[len(tableCols)-1]
				rCols[colName] = 1
			}
			return nil, Goon
		},
	}
	stmts, err := parser.Parse(sql)
	if err != nil {
		return
	}

	_, err = w.Walk(stmts)
	if err != nil {
		return
	}
	for _, col := range w.UnknownNodes {
		log.Warnf("unhandled column type %T", col)
	}
	return
}

//FullColNamesInSelect is not fully accurate.
//
// Deprecated: see function NormalizeAST usage in TestNormalizeASTWithoutInputSchema
func FullColNamesInSelect(sql string) (referredCols ReferredCols, err error) {
	referredCols = make(ReferredCols)

	w := &AstWalker{
		Ctx: referredCols,
		Fn: func(ctx interface{}, node interface{}) (_ interface{}, action Action) {
			rCols := ctx.(ReferredCols)
			if IsColumn(node) {
				nodeName := fmt.Sprint(node)
				rCols[nodeName] = 1
			}
			return nil, Goon
		},
	}
	stmts, err := parser.Parse(sql)
	if err != nil {
		return
	}

	_, err = w.Walk(stmts)
	if err != nil {
		return
	}
	for _, col := range w.UnknownNodes {
		log.Warnf("unhandled column type %T", col)
	}
	return
}
