package lineage

import (
	"fmt"
	"sort"
	"testing"

	"github.com/auxten/go-sql-lineage/test/sql"
	"github.com/auxten/go-sql-lineage/utils"
	"github.com/auxten/postgresql-parser/pkg/sql/sem/tree"
	log "github.com/sirupsen/logrus"
	. "github.com/smartystreets/goconvey/convey"
)

var testCases = []struct {
	sql       string
	cols      []string
	tableCols []string
	err       interface{}
}{
	{"SELECT a.r1, a.r2 FROM a ORDER BY a.r3 LIMIT 1", []string{"r1", "r2", "r3"}, []string{"a.r1", "a.r2", "a.r3"}, nil},
	{"SELECT SUBSTR(t1.TRANS_DATE, 0, 10) as trans_date, t1.TRANS_BRAN_CODE as trans_bran_code,ROUND(SUM(t1.TANS_AMT)/10000,2) as balance, count(t1.rowid) as cnt FROM mj t1 WHERE t1.MC_TRSCODE in ('INQ', 'LIS', 'CWD', 'CDP', 'TFR', 'PIN', 'REP', 'PAY') AND t1.TRANS_FLAG = '0' GROUP BY SUBSTR(t1.TRANS_DATE, 0, 10),t1.TRANS_BRAN_CODE ORDER by trans_date;", []string{"mc_trscode", "rowid", "tans_amt", "trans_bran_code", "trans_date", "trans_flag"}, []string{"mj.mc_trscode", "mj.rowid", "mj.tans_amt", "mj.trans_bran_code", "mj.trans_date", "mj.trans_flag", "trans_date", "mj.trans_flag"}, nil},
	{"SELECT count(DISTINCT s_i_id) FROM order_line JOIN stock ON s_i_id=ol_i_id AND s_w_id=ol_w_id WHERE ol_w_id = $1 AND ol_d_id = $2 AND ol_o_id BETWEEN $3 - 20 AND $3 - 1 AND s_quantity < $4", []string{"s_i_id", "ol_i_id", "s_w_id", "ol_w_id", "ol_d_id", "ol_o_id", "s_quantity"}, []string{"s_i_id", "ol_i_id", "s_w_id", "ol_w_id", "ol_d_id", "ol_o_id", "s_quantity"}, nil},
	{"SELECT c.i / j FROM a AS c JOIN b ON true;", []string{"i", "j"}, []string{"a.i", "j"}, nil},
	{"WITH  with_t1 (c1, c2)   AS (    SELECT 1, 1/0   ) SELECT * FROM with_t1", []string{"*"}, []string{"*"}, "star(*) is not allowed, *"},
	{`select marr
			from (select marr_stat_cd AS marr, label AS l
				  from root_loan_mock_v4
				  order by root_loan_mock_v4.age desc, l desc
				  limit 5) as v4
			RIGHT JOIN
				table1 AS  v3
			ON NULL
			ORDER BY v3.sex DESC
			LIMIT 1;`,
		[]string{"age", "l", "label", "marr", "marr_stat_cd", "sex"}, []string{"root_loan_mock_v4.age", "root_loan_mock_v4.label", "root_loan_mock_v4.marr_stat_cd", "table1.sex", "v4.marr"}, nil},
	{"SELECT TAB_1222.COL1   AS  COL_3470 ,   TAB_1222.COL0   AS  COL_3471 ,   TAB_1222.COL3   AS  COL_3472 FROM TABLE3 TAB_1222 LIMIT 5", []string{"col0", "col1", "col3"}, []string{"table3.col0", "table3.col1", "table3.col3"}, nil},
	{`SELECT
				SET_MASKLEN('4ac:ded4/43', TAB_1226.COL5) AS  COL_3476
			FROM
				PUBLIC.TABLE1 AS  TAB_1223
			RIGHT JOIN
				(
					SELECT
						TAB_1222.COL1   AS  COL_3470
					,   TAB_1222.COL0   AS  COL_3471
					,   TAB_1222.COL3   AS  COL_3472
					FROM TABLE3 TAB_1222
					LIMIT 5
				)   AS  TAB_1224
			(COL_3473, COL_3474, COL_3475)
			JOIN
				TABLE0 AS  TAB_1225
			RIGHT JOIN
				TABLE4 AS  TAB_1226
			ON
				NULL
			FULL JOIN
				PUBLIC.TABLE2 AS  TAB_1227
			ON
				FALSE   ON  SIMILAR_TO_ESCAPE(TAB_1222.COL6, NULL, TAB_1222.COL6)   ON
					INET_CONTAINS_OR_EQUALS(SET_MASKLEN('198f:60f5:287a:8163:c091:2a95:afdc:ae8b/108',
				(-4475677368810664623)), '6d38:61ce:1af7:9283:cf0d:beb2:23e0:d7f/109')
			ORDER BY
				TAB_1226.COL7   DESC
			LIMIT 1`, []string{"col0", "col1", "col3", "col5", "col6", "col7"},
		[]string{"table3.col0", "table3.col1", "table3.col3", "table3.col6", "table4.col5", "table4.col7"}, nil},
	{`WITH
			with_273 (col_3485, col_3486, col_3487, col_3488, col_3489, col_3490, col_3491, col_3492, col_3493)
				AS (
					SELECT
						(-6623365040095722935)  AS col_3485,
						false AS col_3486,
						tab_1229.col1 AS col_3487,
						tab_1229.col6 AS col_3488,
						'1993-06-15'  AS col_3489,
						'rV'  AS col_3490,
						tab_1229.col4 AS col_3491,
						B'0110011001100' AS col_3492,
						tab_1229.col0 AS col_3493
					FROM
						(
							SELECT
								tab_1222.col5 AS col_3477,
								tab_1222.col4 AS col_3478,
								max(tab_1222.col3 )  AS col_3479,
								stddev(tab_1222.col5 )  AS col_3480
							FROM
								table2 tab_1222
							WHERE
								false
							GROUP BY
								tab_1222.col0, tab_1222.col4, tab_1222.col5, tab_1222.col3
							HAVING
								inet_same_family(((
									SELECT
										SET_MASKLEN('4ac:ded4:393a:a371:7690:9d0f:4817:3371/43', TAB_1226.COL5) AS  COL_3476
									FROM
										TABLE1 AS  TAB_1223
									RIGHT JOIN
										(
											SELECT
												COL1   AS  COL_3470
											,   COL0   AS  COL_3471
											,   COL3   AS  COL_3472
											FROM TABLE3
											LIMIT 5
										)   AS  TAB_1224
									(COL_3473, COL_3474, COL_3475)
									JOIN
										TABLE0 AS  TAB_1225
									RIGHT JOIN
										TABLE1 AS  TAB_1226
									ON
										NULL
									FULL JOIN
										TABLE1 AS  TAB_1227
									ON
										FALSE
									ON  SIMILAR_TO_ESCAPE(TAB_1223.COL6, NULL, TAB_1223.COL6)
									ON  INET_CONTAINS_OR_EQUALS(
											SET_MASKLEN('198f:60f5:287a:8163:c091:2a95:afdc:ae8b/108',
												(-4475677368810664623)),
											'6d38:61ce:1af7:9283:cf0d:beb2:23e0:d7f/109')
									ORDER BY
										TAB_1226.COL7   DESC
									LIMIT 1
								)  - tab_1222.col5 )  , NULL )
						)
							AS tab_1228 (col_3481, col_3482, col_3483, col_3484),
						table1 AS tab_1229,
						table1 AS tab_1230
				)
		SELECT
			'c'  AS col_3494,
			e'\x00'  AS col_3495,
			tab_1232.col2 AS col_3496,
			tab_1232.col3 AS col_3497,
			3.4028234663852886e+38  AS col_3498,
			NULL AS col_3499
		FROM
			table1 AS tab_1231,
			table0 AS tab_1232,
			with_273,
			table0 AS tab_1233
		WHERE
			with_273.col_3486
		ORDER BY
			tab_1233.col0 DESC, tab_1232.col4 ASC, with_273.col_3490 ASC
		LIMIT
			23 ;`, []string{"col0", "col1", "col2", "col3", "col4", "col5", "col6", "col7", "col_3486", "col_3490"},
		[]string{"table0.col0", "table0.col2", "table0.col3", "table0.col4", "table1.col0", "table1.col1", "table1.col4", "table1.col5", "table1.col6", "table1.col7", "table2.col0", "table2.col3", "table2.col4", "table2.col5", "table3.col0", "table3.col1", "table3.col3", "with_273.col_3486", "with_273.col_3490"}, nil},
	{sql.SQL246, []string{"agreement_category_cd", "agreement_id", "agreement_mdfr", "agreement_medium_rela_type_cd", "agreement_stat_cd", "agt_open_acct_verify_situati", "agter_ident_info_category_cd", "agter_ident_info_content", "agter_nationality_cd", "agter_nm", "agter_tel", "assoc_agreement_id", "cash_pool_group", "category_cd", "ccy_cd", "close_dt", "cur_bal", "customer", "dep_exchg_ind", "deposit_periods", "end_dt", "fcy_spec_acct_id_type", "inform_deposit_category", "int_type_cd", "intr", "item_id", "mature_dt", "medium_id", "open_acct_amt", "party_id", "prod_cd", "sign_dt", "sign_org", "sleep_acct_ind", "src_sys", "st_dt", "st_int_dt", "sub_prod_cd", "term", "term_days", "term_unit_cd", "tid"},
		[]string{"acct_term_temp.agreement_id", "acct_term_temp.term", "acct_term_temp.term_unit_cd", "agreement_cash_pool_temp.cash_pool_group", "agreement_cash_pool_temp.tid", "agreement_item_temp.agreement_category_cd", "agreement_item_temp.agreement_id", "agreement_item_temp.agreement_mdfr", "agreement_item_temp.agreement_stat_cd", "agreement_item_temp.category_cd", "agreement_item_temp.ccy_cd", "agreement_item_temp.close_dt", "agreement_item_temp.cur_bal", "agreement_item_temp.item_id", "agreement_item_temp.mature_dt", "agreement_item_temp.open_acct_amt", "agreement_item_temp.party_id", "agreement_item_temp.sign_dt", "agreement_item_temp.sign_org", "agreement_item_temp.src_sys", "agreement_item_temp.st_int_dt", "deposit_periods", "prod_cd", "s04_zmq_acc_cur.customer", "s04_zmq_acc_cur.tid", "t03_acct.agreement_id", "t03_acct.dep_exchg_ind", "t03_acct.fcy_spec_acct_id_type", "t03_acct.sleep_acct_ind", "t03_agreement_agt_h.agreement_id", "t03_agreement_agt_h.agt_open_acct_verify_situati", "t03_agreement_agt_h.agter_ident_info_category_cd", "t03_agreement_agt_h.agter_ident_info_content", "t03_agreement_agt_h.agter_nationality_cd", "t03_agreement_agt_h.agter_nm", "t03_agreement_agt_h.agter_tel", "t03_agreement_agt_h.end_dt", "t03_agreement_agt_h.st_dt", "t03_agreement_int_h.agreement_id", "t03_agreement_int_h.agreement_mdfr", "t03_agreement_int_h.end_dt", "t03_agreement_int_h.int_type_cd", "t03_agreement_int_h.intr", "t03_agreement_int_h.st_dt", "t03_agreement_medium_rela_h.agreement_id", "t03_agreement_medium_rela_h.agreement_medium_rela_type_cd", "t03_agreement_medium_rela_h.end_dt", "t03_agreement_medium_rela_h.medium_id", "t03_agreement_medium_rela_h.st_dt", "t03_agreement_pty_rela_h_temp.agreement_id", "t03_agreement_pty_rela_h_temp.party_id", "t03_agreement_rela_h_temp.agreement_id", "t03_agreement_rela_h_temp.assoc_agreement_id", "t03_inform_dep_acct.agreement_id", "t03_inform_dep_acct.end_dt", "t03_inform_dep_acct.inform_deposit_category", "t03_inform_dep_acct.st_dt", "term_days"}, nil},
}

func TestParser(t *testing.T) {
	log.SetLevel(log.DebugLevel)
	Convey("SQL to AST", t, func() {
		const sql = "SELECT SUBSTR(t1.TRANS_DATE, 0, 10) as trans_date, t1.TRANS_BRAN_CODE as trans_bran_code,ROUND(SUM(t1.TANS_AMT)/10000,2) as balance, count(t1.rowid) as cnt FROM mj t1 WHERE t1.MC_TRSCODE in ('INQ', 'LIS', 'CWD', 'CDP', 'TFR', 'PIN', 'REP', 'PAY') AND t1.TRANS_FLAG = '0' GROUP BY SUBSTR(t1.TRANS_DATE, 0, 10),t1.TRANS_BRAN_CODE ORDER by trans_date;"
		ast, err := sql2ast(sql)
		So(err, ShouldBeNil)
		So(ast, ShouldNotBeNil)

		referredCols, err := ColNamesInSelect(sql)
		log.Debugf("referredCols %v", referredCols)
		So(err, ShouldBeNil)

		cols := referredCols.ToList()
		So(cols, ShouldResemble, []string{"mc_trscode", "rowid", "tans_amt", "trans_bran_code", "trans_date", "trans_flag"})
	})

	Convey("case when as", t, func() {
		sql := `SELECT CASE
        WHEN t.category_cd = '4012'
            THEN to_number(substr(sub_prod_cd, 4, 1), '9')
        ELSE COALESCE(t6.term, 0)
        END
        AS deposit_periods,
		COALESCE(t6.term, 0) AS coal
		FROM t`
		ast, err := sql2ast(sql)
		So(err, ShouldBeNil)
		So(ast, ShouldNotBeNil)

		referredCols, err := ColNamesInSelect(sql)
		log.Debugf("referredCols %v", referredCols)
		So(err, ShouldBeNil)

		cols := referredCols.ToList()
		So(cols, ShouldResemble, []string{"category_cd", "sub_prod_cd", "term"})
	})
}

func TestReferredVarsInSelectStatement(t *testing.T) {
	log.SetLevel(log.WarnLevel)

	for _, tc := range testCases {
		Convey(tc.sql, t, func() {
			referredCols, _ := func() (ReferredCols, error) {
				return ColNamesInSelect(tc.sql)
			}()
			//So(err, ShouldResemble, tc.err)
			cols := referredCols.ToList()
			sort.Strings(tc.cols)
			So(cols, ShouldResemble, tc.cols)
		})
	}
}

func TestReferredTableColumnsInSelectStatement(t *testing.T) {
	log.SetLevel(log.DebugLevel)

	for _, tc := range testCases {
		Convey(tc.sql, t, func() {
			referredCols1, referredCols2, err := func() (ReferredCols, ColExprs, error) {
				asts, err := sql2ast(tc.sql)
				So(err, ShouldBeNil)
				So(len(asts), ShouldEqual, 1)

				_, inputCols, err := NormalizeAST(asts[0].AST, nil)
				if tc.err == nil {
					So(err, ShouldBeNil)
				} else {
					So(err.Error(), ShouldResemble, tc.err)
				}
				if err != nil {
					return nil, nil, err
				}
				normalSql := tree.Pretty(asts[0].AST)
				log.Debugf("normalized sql: %s", normalSql)
				referredCols1, err := FullColNamesInSelect(normalSql)

				return referredCols1, inputCols, err
			}()

			if err == nil {
				cols1 := referredCols1.ToList()
				cols2 := referredCols2.ToList()
				tc.tableCols = utils.SortDeDup(tc.tableCols)
				tc.cols = utils.SortDeDup(tc.cols)
				So(cols1, ShouldResemble, tc.tableCols)
				So(cols2, ShouldResemble, tc.tableCols)

				//for _, col := range cols1 {
				//	So(tc.cols, ShouldContain, col)
				//}
			} else {
				So(err.Error(), ShouldResemble, tc.err)
			}
		})
	}
}

func TestGetTabColsMap(t *testing.T) {
	Convey("GetTabColsMap", t, func() {
		cols := ColExprs{
			{
				Name:     "c1",
				SrcTable: []string{"t1"},
				ColType:  Physical,
			},
			{
				Name:     "c1",
				SrcTable: []string{"t2"},
				ColType:  Physical,
			},
			{
				Name:     "c2",
				SrcTable: []string{"t1"},
				ColType:  Physical,
			},
			{
				Name:     "c3",
				SrcTable: []string{},
				ColType:  Unknown,
			},
			{
				Name:     "cl1",
				SrcTable: []string{"t9"},
				ColType:  Logical,
			},
		}
		tcm := GetTabColsMap(cols, false, true)
		So(fmt.Sprint(tcm), ShouldResemble, "map[:map[c3:false] t1:map[c1:true c2:true] t2:map[c1:true]]")
		tcm = GetTabColsMap(cols, false, false)
		So(fmt.Sprint(tcm), ShouldResemble, "map[t1:map[c1:true c2:true] t2:map[c1:true]]")
		tcm = GetTabColsMap(cols, true, false)
		So(fmt.Sprint(tcm), ShouldResemble, "map[t1:map[c1:true c2:true] t2:map[c1:true] t9:map[cl1:false]]")
	})

}
