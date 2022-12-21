package lineage

import (
	"testing"

	log "github.com/sirupsen/logrus"
	. "github.com/smartystreets/goconvey/convey"
)

func TestSQLNormalizeException(t *testing.T) {
	log.SetLevel(log.DebugLevel)
	Convey("Star in select statement", t, func() {
		sql := "select * from tbl"
		asts, err := sql2ast(sql)
		So(err, ShouldBeNil)
		_, err = FindAlias(asts, nil)
		So(err, ShouldNotBeNil)
	})
}

func TestSQLNormalize(t *testing.T) {
	log.SetLevel(log.DebugLevel)
	Convey("mj sql query", t, func() {
		sql := `
			SELECT
				CLERK.OPER_NO               OPER_NO
			,   CLERK.OPER_NAME             OPER_NAME
			,   CLERK.CERT_NO
			,   SUM(coalesce(ACCT_BAL, 0))    SUM_BAL
			FROM
				COMC_CLERK  CLERK
			INNER JOIN
				COMR_CIFBINFO   CIFO
			ON
				CLERK.CERT_NO =   CIFO.CERT_NO
			INNER JOIN
				(
					SELECT
						CUST_NO
					,   ACCT_NO
					FROM
						SAVB_BASICINFO
				)   AS  BASIC
			ON
				CIFO.CUST_NO  =   BASIC.CUST_NO
			INNER JOIN
				(
					SELECT
						SUB_CODE
					,   ACCT_NO
					,   ACCT_BAL
					FROM
						SAVB_ACCTINFO_CHK
				)   AS  ACCT
			ON
				BASIC.ACCT_NO =   ACCT.ACCT_NO
			AND SUB_CODE =   '21103'
			WHERE
				SUBSTR(OPER_NO, 1, 2) IN  ('f0', 'f1', 'f2', 'f3', 'f4', 'f5', 'f6', 'f7', 'f8', 'f9')
			GROUP BY
				OPER_NO
			,   OPER_NAME
			,   CLERK.CERT_NO
			HAVING
				SUM_BAL >   500000
			LIMIT 1000
		`
		asts, err := sql2ast(sql)
		So(err, ShouldBeNil)
		_, err = FindAlias(asts, nil)
		So(err, ShouldBeNil)
	})
}
