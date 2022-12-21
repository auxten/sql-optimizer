package lineage

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/auxten/go-sql-lineage/test/sql"
	"github.com/auxten/go-sql-lineage/utils"
	"github.com/auxten/postgresql-parser/pkg/sql/sem/tree"
	log "github.com/sirupsen/logrus"
)

func TestGetProvidedColumns(t *testing.T) {
	log.SetLevel(log.DebugLevel)
	var testData = []struct {
		inputSchema Tables
		original    string
		outputCols  string
		outputTabs  string
	}{
		{
			Tables{
				{
					Name: "EMPLOYEES",
					Cols: []Col{
						{Name: "job_id"},
						{Name: "SALARY"},
					},
				},
				{
					Name: "Performance",
					Cols: []Col{
						{Name: "id"},
						{Name: "score"},
					},
				},
			},
			sql.SQLNestedSubquery,
			"[employees.job_id employees.salary p.id p.score maxavg.max(myavg) maxavg.job_id]",
			"[employees p maxavg]",
		},
		{
			Tables{
				{
					Name: "Customers",
					Cols: []Col{
						{Name: "customer"},
						{Name: "ContactName"},
						{Name: "City"},
						{Name: "country"},
					},
				},
			},
			sql.SQLUnion,
			"[type contactname city country]",
			"[]",
		},
		{
			Tables{
				{
					Name: "COMC_CLERK",
					Cols: []Col{
						{Name: "rowid"},
						{Name: "oper_no"},
						{Name: "oper_name"},
						{Name: "cert_no"},
						{Name: "oper_no"},
					},
				},
				{
					Name: "comr_cifbinfo",
					Cols: []Col{
						{Name: "rowid"},
						{Name: "cert_no"},
						{Name: "cust_no"},
					},
				},
				{
					Name: "savb_basicinfo",
					Cols: []Col{
						{Name: "rowid"},
						{Name: "cust_no"},
						{Name: "acct_no"},
						{Name: "unnecessary"},
					},
				},
				{
					Name: "savb_acctinfo_chk",
					Cols: []Col{
						{Name: "rowid"},
						{Name: "sub_code"},
						{Name: "acct_no"},
						{Name: "acct_bal"},
					},
				},
			},
			sql.SQLmj,
			"[clerk.rowid clerk.oper_no clerk.oper_name clerk.cert_no clerk.oper_no cifo.rowid cifo.cert_no cifo.cust_no basic.cust_no basic.acct_no basic.unnecessary acct.sub_code acct.acct_no acct.acct_bal]",
			"[clerk cifo basic acct]",
		},
	}

	for i, d := range testData {
		t.Run(fmt.Sprintf("get provided columns %d", i), func(t *testing.T) {
			asts, err := sql2ast(d.original)
			if err != nil {
				t.Fatal(err)
			}
			if len(asts) != 1 {
				t.Fatal(asts)
			}

			var subqueryOrTable interface{}
			switch clause := asts[0].AST.(*tree.Select).Select.(type) {
			case *tree.SelectClause:
				subqueryOrTable = clause.From.Tables
			case *tree.UnionClause:
				subqueryOrTable = clause
			}

			schemaStr, _ := json.Marshal(d.inputSchema)
			fmt.Printf("input schema: %s", schemaStr)

			cols, tabs := GetInputColumns(d.inputSchema, subqueryOrTable)

			diff, _ := utils.Diff("TestGetProvidedTables",
				[]byte(fmt.Sprint(tabs)),
				[]byte(fmt.Sprint(d.outputTabs)),
			)
			if len(diff) > 0 {
				t.Error(string(diff))
			}

			diff, _ = utils.Diff("TestGetProvidedColumns",
				[]byte(fmt.Sprint(cols)),
				[]byte(fmt.Sprint(d.outputCols)),
			)
			if len(diff) > 0 {
				t.Error(string(diff))
			}
		})
	}
}
