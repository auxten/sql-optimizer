package lineage

import (
	"fmt"
	"testing"

	"github.com/auxten/go-sql-lineage/test/sql"
	"github.com/auxten/go-sql-lineage/utils"
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
			[]string{"a", "b", "ct4", "e", "t31.c", "t31.d"},
		},
		{
			sql.SQLmj,
			sql.SQLmjNormal,
			[]string{"comc_clerk.cert_no", "comc_clerk.oper_name", "comc_clerk.oper_no", "comr_cifbinfo.cert_no", "comr_cifbinfo.cust_no", "savb_acctinfo_chk.acct_bal", "savb_acctinfo_chk.acct_no", "savb_acctinfo_chk.sub_code", "savb_basicinfo.acct_no", "savb_basicinfo.cust_no", "savb_basicinfo.unnecessary"},
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
			[]string{"root_loan_mock_v4.age", "root_loan_mock_v4.label", "root_loan_mock_v4.marr_stat_cd", "table1.sex"},
		},
		{
			sql.SQL246,
			sql.SQL246Normal,
			[]string{"acct_term_temp.agreement_id",
				"acct_term_temp.term",
				"acct_term_temp.term_unit_cd",
				"agreement_cash_pool_temp.cash_pool_group",
				"agreement_cash_pool_temp.tid",
				"agreement_item_temp.agreement_category_cd",
				"agreement_item_temp.agreement_id",
				"agreement_item_temp.agreement_mdfr",
				"agreement_item_temp.agreement_stat_cd",
				"agreement_item_temp.category_cd",
				"agreement_item_temp.ccy_cd",
				"agreement_item_temp.close_dt",
				"agreement_item_temp.cur_bal",
				"agreement_item_temp.item_id",
				"agreement_item_temp.mature_dt",
				"agreement_item_temp.open_acct_amt",
				"agreement_item_temp.party_id",
				"agreement_item_temp.sign_dt",
				"agreement_item_temp.sign_org",
				"agreement_item_temp.src_sys",
				"agreement_item_temp.st_int_dt",
				"s04_zmq_acc_cur.customer",
				"s04_zmq_acc_cur.tid",
				"t03_acct.agreement_id",
				"t03_acct.dep_exchg_ind",
				"t03_acct.fcy_spec_acct_id_type",
				"t03_acct.sleep_acct_ind",
				"t03_agreement_agt_h.agreement_id",
				"t03_agreement_agt_h.agt_open_acct_verify_situati",
				"t03_agreement_agt_h.agter_ident_info_category_cd",
				"t03_agreement_agt_h.agter_ident_info_content",
				"t03_agreement_agt_h.agter_nationality_cd",
				"t03_agreement_agt_h.agter_nm",
				"t03_agreement_agt_h.agter_tel",
				"t03_agreement_agt_h.end_dt",
				"t03_agreement_agt_h.st_dt",
				"t03_agreement_int_h.agreement_id",
				"t03_agreement_int_h.agreement_mdfr",
				"t03_agreement_int_h.end_dt",
				"t03_agreement_int_h.int_type_cd",
				"t03_agreement_int_h.intr",
				"t03_agreement_int_h.st_dt",
				"t03_agreement_medium_rela_h.agreement_id",
				"t03_agreement_medium_rela_h.agreement_medium_rela_type_cd",
				"t03_agreement_medium_rela_h.end_dt",
				"t03_agreement_medium_rela_h.medium_id",
				"t03_agreement_medium_rela_h.st_dt",
				"t03_agreement_pty_rela_h_temp.agreement_id",
				"t03_agreement_pty_rela_h_temp.party_id",
				"t03_agreement_rela_h_temp.agreement_id",
				"t03_agreement_rela_h_temp.assoc_agreement_id",
				"t03_inform_dep_acct.agreement_id",
				"t03_inform_dep_acct.end_dt",
				"t03_inform_dep_acct.inform_deposit_category",
				"t03_inform_dep_acct.st_dt"},
		},
		{
			original: sql.SQLNestedSubquery,
			normal:   sql.SQLNestedSubqueryNormal,
			referredCols: []string{"employees.job_id",
				"job_history.department_id",
				"job_history.job_id",
				"jobs.job_id",
				"jobs.min_salary",
				"performance.id",
				"performance.score",
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
