/*
Package optimizer optimizes the whole work flow by analyzing the target SQL.

# How the Optimizer Works

1. Scan AST for all tables and columns alias ("AS" or without "AS")
2. Normalize all tables and columns in sub AST.
3. Call the optimizers one by one. Optimizers will:
	1. Walk the complete AST with context.
	2. Calculate the costs of all possible transformations.
	3. Modify the input schema and the target SQL query to minimize the cost.

# Available Optimizers

1. Find all the columns referenced in the target SQL. Check the data source schema.
	1. If there are columns in the source schema, but not used in the target SQL. Omit it in the engine.
	2. Columns in the target SQL not available in source schema. Raise an error.
2. Find join conditions that introducing any function, add the function result as a new column
	and the replace the join condition with simple value equality.
	eg:

	```sql
	SELECT name, sex FROM t1 JOIN t2
		ON trim(t1.id) = trim(t2.id);
	```
	add column `t1.id_trim` and `t2.id_trim` during source table maintenance,
	and the modified target SQL will be:

	```sql
	SELECT name, sex FROM t1 JOIN t2
		ON t1.id_trim = t2.id_trim;
	```

## Table/Column name normalization

Targets:

1. Column prefixed by table alias or table name omitted will be normalized by full table.column name.
2. Column alias in SELECT projection will be preserved, but column alias referenced in other parts like
	WHERE/HAVING clause will be normalized.
3. Table alias from subquery will be preserved.

Most column references are implicit, to cover all the normalization cases. These preconditions are necessary:

1. Source schema without ambiguity. eg:
	SQL like `SELECT name, sex FROM t1 JOIN t2 ...` can not run on condition
	that both t1 and t2 have column `name`.
2. Although SQL standards support same alias name in one SQL statement, but I don't want to support that. eg:

	```sql
	SELECT distinct(typ)
	FROM
	  (
		SELECT b1.resource_type typ FROM Branding b1
			UNION ALL
		SELECT b1.payload_type typ FROM Payloads b1
	  );
	```
	SQL like above will cause an exception.
3. The star(*) in SELECT projection which will cause output schema undetermined before execution is not allowed.
4. In [SQL:2011 standard](https://jakewheat.github.io/sql-overview/sql-2011-foundation-grammar.html#_7_6_table_reference),
	table alias with a column list is allowed. This will make things much more complicated.
	So things like 'AS name(col1, col2)' will also cause an exception.


*/

package lineage

import (
	"encoding/json"

	"github.com/auxten/postgresql-parser/pkg/sql/parser"
	"github.com/auxten/postgresql-parser/pkg/sql/sem/tree"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

// Table is the description of table in ngnx.
type Table struct {
	Name string
	// Pk is the primary key name.
	Pk string
	// Cols are the columns in the Table
	Cols []Col
	// Index is the column names array to build an index
	Index []string
}

type Col struct {
	// Name is the column name
	Name string
	// Type is the column schema type
	Type string
	//// Extra column description, eg. UNIQUE/DEFAULT
	//Extra string
	//// PreProcess is the function applied before INSERT or UPDATE.
	//// Example: trim(%s) will produce trim(colName) in SQL.
	//PreProcess string
	//// InsertPath is the json path of INSERT column data
	//InsertPath string
	//// UpdatePath is the json path of UPDATE column data
	//UpdatePath string
}

type Schema struct {
	Name   string
	Engine string
	Query  string
	//Physical table columns referred in Query
	ReferredColumns map[string]map[string]bool // map[table][column]bool
	Tables          Tables
	TableMap        map[string]*Table `yaml:"-"` // tableName:table
}

type Tables []Table
type DataSource interface{}

func UnmarshalSchema(data []byte) (t Tables, err error) {
	var tables []Table
	err = json.Unmarshal(data, &tables)
	if err != nil {
		return nil, err
	}
	for _, table := range tables {
		t = append(t, table)
	}
	return t, nil
}

type QueryCtx struct {
	current  int
	Contexts []SubqueryCtx
	Err      error
}

type View struct {
	TableView    *TableStmt
	SubqueryView *SubqueryCtx
	NodeRef      interface{} // Node pointer to AST tree node
}

type SubqueryCtx struct {
	//Node tree.
	// Projections are the query output schema
	Projections []tree.SelectExpr
	// From contains the input table schema and subquery schema
	// we call these two types View
	From []View
	// Alias is the subquery alias name
	Alias string
}

// TableStmt is the table referenced in the SQL.
type TableStmt struct {
	Name    tree.NameParts
	Columns map[string]ColumnStmt // map[ColumnName or AliasName]ColumnStmt
	Alias   string
}

// ColumnStmt is the column referenced in the SQL.
type ColumnStmt struct {
	Name  string
	Alias string
	Typ   string
}

func (qc *QueryCtx) CurSubContext() *SubqueryCtx {
	return &qc.Contexts[qc.current]
}

func (qc *QueryCtx) NewSubContext() *SubqueryCtx {
	qc.Contexts = append(qc.Contexts, SubqueryCtx{})
	qc.current++
	return qc.CurSubContext()
}

// FindAlias collects all tables and columns alias, the walker function will keep a subquery level context.
// As a result, this func is NOT CONCURRENT SAFE.
func FindAlias(stmts parser.Statements, inputSchema *Schema) (ctx *QueryCtx, err error) {
	ctx = &QueryCtx{}
	w := &AstWalker{
		Ctx: ctx,
		Fn: func(ctx interface{}, node interface{}) (_ interface{}, action Action) {
			queryCtx := ctx.(*QueryCtx)

			switch expr := node.(type) {
			case *tree.Select:
				// first entrance
				if queryCtx.Contexts == nil {
					queryCtx.Contexts = make([]SubqueryCtx, 1)
				}
			case *tree.Subquery:
			case *tree.AliasedTableExpr:
				if _, ok := expr.Expr.(*tree.Subquery); ok {
					// Subquery as a table alias
					curSubContext := queryCtx.NewSubContext()
					if len(expr.As.Alias) != 0 {
						curSubContext.Alias = string(expr.As.Alias)
					}
					if len(expr.As.Cols) != 0 {
						// See precondition #4
						queryCtx.Err = errors.Errorf("as with column list is not supported %s", expr.As)
						log.Error(queryCtx.Err)
						return nil, Stop
					}
					log.Debugf("new subquery %s", node)
				}
				// else Ordinary table alias

				// TODO
				log.Infof("table %s -> %s", expr.As.Alias, expr.Expr)
			case *tree.SelectExpr:
				if len(expr.As) != 0 {
					log.Infof("column %s -> %s", expr.As, expr.Expr)
					//log.Debugf("%s", expr.NormalizeTopLevelVarName())
				}
				return nil, Goon
			case tree.UnqualifiedStar:
				// See precondition #3
				queryCtx.Err = errors.Errorf("star projection encountered in node: %s", node)
				log.Errorf("star column is not allowed %v", queryCtx.Err)
				return nil, Stop
			}

			if expr, ok := node.(*tree.UnresolvedName); ok {
				if expr.Star {
					// See precondition #3
					queryCtx.Err = errors.Errorf("star projection encountered in node: %s", node)
					log.Errorf("star column is not allowed %v", queryCtx.Err)
					return nil, Stop
				}
			}

			//log.Debugf("%s\n", node)
			return nil, Goon
		},
	}

	_, err = w.Walk(stmts)
	if err != nil {
		return
	}
	if ctx.Err != nil {
		err = ctx.Err
		return
	}

	return
}
