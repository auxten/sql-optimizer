package lineage

import (
	"fmt"
	"testing"

	"github.com/auxten/sql-optimizer/test/sql"
	"github.com/auxten/sql-optimizer/utils"
	"github.com/auxten/postgresql-parser/pkg/sql/sem/tree"
	log "github.com/sirupsen/logrus"
	. "github.com/smartystreets/goconvey/convey"
)

func TestNormalizeColumnName(t *testing.T) {
	log.SetLevel(log.DebugLevel)
	Convey("mj sql query", t, func() {
		var table, column string

		asts, err := sql2ast(sql.SQLmj)
		So(err, ShouldBeNil)
		So(len(asts), ShouldEqual, 1)

		treeRange := asts[0].AST.(*tree.Select).Select.(*tree.SelectClause)
		sumBal := asts[0].AST.(*tree.Select).Select.(*tree.SelectClause).Having.Expr.(*tree.ComparisonExpr).Left
		tableAliasList := utils.NewStack(0)
		userCtx := &UserCtx{TableAliasList: tableAliasList, DeferredFuncs: utils.NewStack(0)}
		table, column, err = NormalizeColumnName(sumBal, treeRange, nil, nil, userCtx)
		So(err, ShouldBeNil)
		So(table, ShouldResemble, "")
		So(column, ShouldResemble, "sum_bal")
		So(len(*tableAliasList), ShouldEqual, 0)

		treeRange = asts[0].AST.(*tree.Select).Select.(*tree.SelectClause)
		operNo := asts[0].AST.(*tree.Select).Select.(*tree.SelectClause).GroupBy[0]
		tableAliasList = utils.NewStack(4)
		userCtx = &UserCtx{TableAliasList: tableAliasList, DeferredFuncs: utils.NewStack(0)}
		table, column, err = NormalizeColumnName(operNo, treeRange, nil, nil, userCtx)
		So(err, ShouldBeNil)
		So(table, ShouldResemble, "comc_clerk")
		So(column, ShouldResemble, "oper_no")
		So(tableAliasList.Len(), ShouldEqual, 1)
		tableAlias, _ := tableAliasList.Pop()
		So(tableAlias.(*tree.AliasClause).Alias.String(), ShouldResemble, "clerk")

		treeRange = asts[0].AST.(*tree.Select).Select.(*tree.SelectClause)
		basicAcctNo := asts[0].AST.(*tree.Select).Select.(*tree.SelectClause).From.Tables[0].(*tree.JoinTableExpr).Cond.(*tree.OnJoinCond).Expr.(*tree.AndExpr).Left.(*tree.ComparisonExpr).Left
		tableAliasList = utils.NewStack(0)
		userCtx = &UserCtx{TableAliasList: tableAliasList, DeferredFuncs: utils.NewStack(0)}
		table, column, err = NormalizeColumnName(basicAcctNo, treeRange, nil, nil, userCtx)
		So(err, ShouldBeNil)
		/*
			subquery alias is not normalized
			(
				SELECT
					CUST_NO
				,   ACCT_NO
				FROM
					SAVB_BASICINFO
			)   AS  BASIC

		*/
		So(table, ShouldResemble, "basic")
		So(column, ShouldResemble, "acct_no")
		So(tableAliasList.Len(), ShouldEqual, 0)
	})
}

func TestNormalizeASTWithoutInputSchema(t *testing.T) {
	log.SetLevel(log.DebugLevel)
	var testData = []struct {
		original     string
		normal       string
		referredCols []string
	}{
		{
			sql.SQLMultiFrom,
			sql.SQLMultiFromNormal,
			[]string{"a:Unknown", "b:Unknown", "ct4:Unknown", "e:Unknown", "t31.c:Physical", "t31.d:Physical"},
		},
		{
			sql.SQLmj,
			sql.SQLmjNormal,
			[]string{"comc_clerk.cert_no:Physical", "comc_clerk.oper_name:Physical", "comc_clerk.oper_no:Physical", "comr_cifbinfo.cert_no:Physical", "comr_cifbinfo.cust_no:Physical", "savb_acctinfo_chk.acct_bal:Physical", "savb_acctinfo_chk.acct_no:Physical", "savb_acctinfo_chk.sub_code:Physical", "savb_basicinfo.acct_no:Physical", "savb_basicinfo.cust_no:Physical", "savb_basicinfo.unnecessary:Physical"},
		},
		{
			`select marr
			from (select marr_stat_cd AS marr, label AS l
				  from root_loan_mock_v4
				  order by root_loan_mock_v4.age desc, l desc
				  limit 5) as v4
			RIGHT JOIN
				table1 AS  v3
			ON NULL
			ORDER BY v3.sex DESC
			LIMIT 1;`,
			`SELECT
	v4.marr
FROM
	(
		SELECT
			root_loan_mock_v4.marr_stat_cd AS marr,
			root_loan_mock_v4.label AS l
		FROM
			root_loan_mock_v4
		ORDER BY
			root_loan_mock_v4.age DESC,
			root_loan_mock_v4.label DESC
		LIMIT
			5
	)
		AS v4
	RIGHT JOIN table1 ON NULL
ORDER BY
	table1.sex DESC
LIMIT
	1`,
			[]string{"root_loan_mock_v4.age:Physical", "root_loan_mock_v4.label:Physical", "root_loan_mock_v4.marr_stat_cd:Physical", "table1.sex:Physical"},
		},
		{
			sql.SQL246,
			sql.SQL246Normal,
			[]string{"acct_term_temp.agreement_id:Physical",
				"acct_term_temp.term:Physical",
				"acct_term_temp.term_unit_cd:Physical",
				"agreement_cash_pool_temp.cash_pool_group:Physical",
				"agreement_cash_pool_temp.tid:Physical",
				"agreement_item_temp.agreement_category_cd:Physical",
				"agreement_item_temp.agreement_id:Physical",
				"agreement_item_temp.agreement_mdfr:Physical",
				"agreement_item_temp.agreement_stat_cd:Physical",
				"agreement_item_temp.category_cd:Physical",
				"agreement_item_temp.ccy_cd:Physical",
				"agreement_item_temp.close_dt:Physical",
				"agreement_item_temp.cur_bal:Physical",
				"agreement_item_temp.item_id:Physical",
				"agreement_item_temp.mature_dt:Physical",
				"agreement_item_temp.open_acct_amt:Physical",
				"agreement_item_temp.party_id:Physical",
				"agreement_item_temp.sign_dt:Physical",
				"agreement_item_temp.sign_org:Physical",
				"agreement_item_temp.src_sys:Physical",
				"agreement_item_temp.st_int_dt:Physical",
				"s04_zmq_acc_cur.customer:Physical",
				"s04_zmq_acc_cur.tid:Physical",
				"t03_acct.agreement_id:Physical",
				"t03_acct.dep_exchg_ind:Physical",
				"t03_acct.fcy_spec_acct_id_type:Physical",
				"t03_acct.sleep_acct_ind:Physical",
				"t03_agreement_agt_h.agreement_id:Physical",
				"t03_agreement_agt_h.agt_open_acct_verify_situati:Physical",
				"t03_agreement_agt_h.agter_ident_info_category_cd:Physical",
				"t03_agreement_agt_h.agter_ident_info_content:Physical",
				"t03_agreement_agt_h.agter_nationality_cd:Physical",
				"t03_agreement_agt_h.agter_nm:Physical",
				"t03_agreement_agt_h.agter_tel:Physical",
				"t03_agreement_agt_h.end_dt:Physical",
				"t03_agreement_agt_h.st_dt:Physical",
				"t03_agreement_int_h.agreement_id:Physical",
				"t03_agreement_int_h.agreement_mdfr:Physical",
				"t03_agreement_int_h.end_dt:Physical",
				"t03_agreement_int_h.int_type_cd:Physical",
				"t03_agreement_int_h.intr:Physical",
				"t03_agreement_int_h.st_dt:Physical",
				"t03_agreement_medium_rela_h.agreement_id:Physical",
				"t03_agreement_medium_rela_h.agreement_medium_rela_type_cd:Physical",
				"t03_agreement_medium_rela_h.end_dt:Physical",
				"t03_agreement_medium_rela_h.medium_id:Physical",
				"t03_agreement_medium_rela_h.st_dt:Physical",
				"t03_agreement_pty_rela_h_temp.agreement_id:Physical",
				"t03_agreement_pty_rela_h_temp.party_id:Physical",
				"t03_agreement_rela_h_temp.agreement_id:Physical",
				"t03_agreement_rela_h_temp.assoc_agreement_id:Physical",
				"t03_inform_dep_acct.agreement_id:Physical",
				"t03_inform_dep_acct.end_dt:Physical",
				"t03_inform_dep_acct.inform_deposit_category:Physical",
				"t03_inform_dep_acct.st_dt:Physical",
			},
		},
		{
			original: sql.SQLNestedSubquery,
			normal:   sql.SQLNestedSubqueryNormal,
			referredCols: []string{"employees.job_id:Physical",
				"job_history.department_id:Physical",
				"job_history.job_id:Physical",
				"jobs.job_id:Physical",
				"jobs.min_salary:Physical",
				"performance.id:Physical",
				"performance.score:Physical",
			},
		},
	}
	for i, d := range testData {
		t.Run(fmt.Sprintf("Normalize whole AST %d", i), func(t *testing.T) {
			asts, err := sql2ast(d.original)
			if err != nil {
				t.Fatal(err)
			}
			if len(asts) != 1 {
				t.Fatal(asts)
			}

			_, inputCols, err := NormalizeAST(asts[0].AST, nil)
			if err != nil {
				t.Fatal(err)
			}

			log.Debug(tree.Pretty(asts[0].AST))

			diff, _ := utils.Diff("test", []byte(tree.Pretty(asts[0].AST)), []byte(d.normal))
			if len(diff) > 0 {
				t.Fatal(string(diff))
			}

			colsFromAST := make([]string, 0, len(inputCols))
			for _, col := range inputCols {
				//if col.ExprType == Physical || col.ExprType == Unknown {
				if col.ColType == Physical ||
					(col.ColType == Unknown && len(col.SrcTable) == 0) {
					colsFromAST = append(colsFromAST, col.String())
				}
			}

			diff, _ = utils.Diff("testReferred",
				[]byte(fmt.Sprint(utils.SortDeDup(colsFromAST))),
				[]byte(fmt.Sprint(utils.SortDeDup(d.referredCols))),
			)
			if len(diff) > 0 {
				t.Fatal(string(diff))
			}
		})
	}
}
