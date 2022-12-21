package main

import (
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/auxten/go-sql-lineage/lineage"
	"github.com/auxten/postgresql-parser/pkg/sql/parser"
	"github.com/auxten/postgresql-parser/pkg/sql/sem/tree"
)

func main() {
	// -sql option gets SQL to analysis
	// -schema option gets table schema, it's optional, represents in JSON format
	var (
		err         error
		inputSchema lineage.Tables
		sqlStr      string
		schemaStr   string
		help        bool
	)

	flag.StringVar(&sqlStr, "sql", "", "SQL to analysis, read SQL from stdin is also supported")
	flag.StringVar(&schemaStr, "schema", "", "Table schema, optional, represents in JSON format")
	flag.BoolVar(&help, "help", false, "Show help")

	flag.Parse()

	if sqlStr == "" {
		// try to read SQL str from stdin
		sqlBytes, _ := io.ReadAll(os.Stdin)
		sqlStr = string(sqlBytes)
	}
	if sqlStr == "" {
		fmt.Println("SQL to analysis is required")
		help = true
	}
	if help {
		flag.Usage()
		return
	}
	if schemaStr != "" {
		inputSchema, err = lineage.UnmarshalSchema([]byte(schemaStr))
		if err != nil {
			panic(err)
		}
	}

	// get AST from SQL
	asts, err := parser.Parse(sqlStr)
	if err != nil {
		panic(err)
	}
	if len(asts) != 1 {
		panic("Only support single SQL")
	}
	// normalize SQL
	normalizedAst, inputCols, err := lineage.NormalizeAST(asts[0].AST, inputSchema)
	if err != nil {
		panic(err)
	}
	// print normalized SQL
	fmt.Printf("Normalized SQL: %s\n", tree.Pretty(normalizedAst))
	fmt.Printf("SQL Refered Columns: %s\n", inputCols)

	// get output table schema
	var subqueryOrTable interface{}
	switch clause := normalizedAst.(*tree.Select).Select.(type) {
	case *tree.SelectClause:
		subqueryOrTable = clause.From.Tables
	case *tree.UnionClause:
		subqueryOrTable = clause
	}
	cols, tabs := lineage.GetInputColumns(inputSchema, subqueryOrTable)
	// print output table schema
	fmt.Printf("SQL Input Tables: %s\n", tabs)
	fmt.Printf("SQL Input Columns: %s\n", cols)

	outputCols := lineage.GetOutputColumns(inputSchema, normalizedAst.(*tree.Select).Select.(*tree.SelectClause))
	fmt.Printf("SQL Output Columns: %s\n", outputCols)
}
