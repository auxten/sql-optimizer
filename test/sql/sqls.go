package sql

var (
	SQLMultiFrom = `
SELECT
    A
,   B
,   C
,   D
,   E
,   CT4
FROM
    T1 AS t1a
,   T2
JOIN
    T21 USING
    (A, B)
,   (
        SELECT
            C
        ,   D
        FROM
            T31
    )       AS  T3
,   T4`

	SQLMultiFromNormal = `SELECT
	a, b, t3.c, t3.d, e, ct4
FROM
	t1 AS t1a,
	t2
	JOIN t21 USING (a, b),
	(SELECT t31.c, t31.d FROM t31) AS t3,
	t4`

	SQLNestedSubquery = `
SELECT JOB_ID ,
       score,
       AVG(SALARY)
FROM EMPLOYEES
LEFT JOIN
  (SELECT id,
          score
   FROM Performance) AS P USING (id)
LEFT JOIN
  (SELECT MAX(MYAVG), JOB_ID
   FROM
     (SELECT JOB_ID ,
             AVG(MIN_SALARY) AS MYAVG
      FROM JOBS
      WHERE JOB_ID IN
          (SELECT JOB_ID
           FROM JOB_HISTORY
           WHERE DEPARTMENT_ID BETWEEN 50 AND 100)
      GROUP BY JOB_ID) SS) AS maxavg ON SS.JOB_ID = EMPLOYEES.JOB_ID
GROUP BY JOB_ID HAVING AVG(SALARY) <
  (SELECT MAX(MYAVG)
   FROM
     (SELECT JOB_ID ,
             AVG(MIN_SALARY) AS MYAVG
      FROM JOBS
      WHERE JOB_ID IN
          (SELECT JOB_ID
           FROM JOB_HISTORY
           WHERE DEPARTMENT_ID BETWEEN 50 AND 100)
      GROUP BY JOB_ID) SS) ;`

	SQLNestedSubqueryNormal = `SELECT
	maxavg.job_id, p.score, avg(salary)
FROM
	employees
	LEFT JOIN (
			SELECT
				performance.id, performance.score
			FROM
				performance
		)
			AS p USING (id)
	LEFT JOIN (
			SELECT
				max(ss.myavg), ss.job_id
			FROM
				(
					SELECT
						jobs.job_id,
						avg(jobs.min_salary) AS myavg
					FROM
						jobs
					WHERE
						jobs.job_id
						IN (
								SELECT
									job_history.job_id
								FROM
									job_history
								WHERE
									job_history.department_id BETWEEN 50 AND 100
							)
					GROUP BY
						jobs.job_id
				)
					AS ss
		)
			AS maxavg ON ss.job_id = employees.job_id
GROUP BY
	maxavg.job_id
HAVING
	avg(salary)
	< (
			SELECT
				max(ss.myavg)
			FROM
				(
					SELECT
						jobs.job_id,
						avg(jobs.min_salary) AS myavg
					FROM
						jobs
					WHERE
						jobs.job_id
						IN (
								SELECT
									job_history.job_id
								FROM
									job_history
								WHERE
									job_history.department_id BETWEEN 50 AND 100
							)
					GROUP BY
						jobs.job_id
				)
					AS ss
		)`
	SQLUnion = `
SELECT 'Customer' AS Type, ContactName, City, Country
FROM Customers
UNION
SELECT 'Supplier', ContactName, City, Country
FROM Suppliers;`
	SQLmj = `
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
				,   UNNECESSARY
				FROM
					SAVB_BASICINFO
			)   AS  BASIC
		ON
			CIFO.CUST_NO  =   BASIC.CUST_NO
		INNER JOIN
			(
				SELECT
					CHK.SUB_CODE
				,   CHK.ACCT_NO
				,   CHK.ACCT_BAL
				FROM
					SAVB_ACCTINFO_CHK CHK
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
	SQLmjNormal = `SELECT
	comc_clerk.oper_no AS oper_no,
	comc_clerk.oper_name AS oper_name,
	comc_clerk.cert_no,
	sum(COALESCE(acct.acct_bal, 0)) AS sum_bal
FROM
	comc_clerk
	INNER JOIN comr_cifbinfo ON
			comc_clerk.cert_no = comr_cifbinfo.cert_no
	INNER JOIN (
			SELECT
				savb_basicinfo.cust_no,
				savb_basicinfo.acct_no,
				savb_basicinfo.unnecessary
			FROM
				savb_basicinfo
		)
			AS basic ON
			comr_cifbinfo.cust_no = basic.cust_no
	INNER JOIN (
			SELECT
				savb_acctinfo_chk.sub_code,
				savb_acctinfo_chk.acct_no,
				savb_acctinfo_chk.acct_bal
			FROM
				savb_acctinfo_chk
		)
			AS acct ON
			basic.acct_no = acct.acct_no
			AND acct.sub_code = '21103'
WHERE
	substr(comc_clerk.oper_no, 1, 2)
	IN (
			'f0',
			'f1',
			'f2',
			'f3',
			'f4',
			'f5',
			'f6',
			'f7',
			'f8',
			'f9'
		)
GROUP BY
	comc_clerk.oper_no,
	comc_clerk.oper_name,
	comc_clerk.cert_no
HAVING
	sum_bal > 500000
LIMIT
	1000`
	SQL246 = `SELECT
        to_date('20011230', 'YYYYMMDD') AS data_dt,
        COALESCE(t4.party_id, t.sign_org) AS org_id,
        t.agreement_id,
        t1.fcy_spec_acct_id_type AS fcy_spec_acct_id_type,
        t.agreement_mdfr,
        t.category_cd,
        COALESCE(NULL, '') AS retail_ind,
        t.item_id,
        CASE
        WHEN t.category_cd IN ('1013', '1021') THEN '1101'
        WHEN t.category_cd IN ('4009',) THEN '1102'
        WHEN t.category_cd IN ('4013',) THEN '1201'
        WHEN t.category_cd IN ('4010',)
        AND t3.inform_deposit_category IN ('SLA', 'EDA')
        THEN '1202'
        WHEN t.category_cd IN ('4012',)
        AND t3.inform_deposit_category
                NOT IN ('NRI1', 'NRI7', 'IMM7', 'REN7')
        THEN '1301'
        WHEN t.category_cd IN ('4012',)
        AND t3.inform_deposit_category IN ('NRI1', 'NRI7')
        THEN '1302'
        WHEN t.category_cd IN ('4012',)
        AND t3.inform_deposit_category IN ('IMM7', 'REN7')
        THEN '1303'
        ELSE '9999'
        END
                AS prod_cd,
        t3.inform_deposit_category AS sub_prod_cd,
        t.party_id AS cust_id,
        t.category_cd AS acct_category_cd,
        t.agreement_category_cd AS acct_type_cd,
        CASE
        WHEN t.category_cd = '1604' THEN '2'
        ELSE '1'
        END
                AS acct_stat_cd,
        CASE
        WHEN t1.sleep_acct_ind = 'Y' THEN '1'
        ELSE '0'
        END
                AS dormancy_ind,
        CASE
        WHEN t1.dep_exchg_ind = 'Y' THEN '1'
        ELSE '0'
        END
                AS dep_exchg_ind,
        COALESCE(t11.intr, 0.00) AS mature_intr,
        COALESCE(t.sign_dt, to_date('${NULLDATE}', 'YYYYMMDD'))
                AS open_acct_dt,
        COALESCE(
                t.st_int_dt,
                to_date('${NULLDATE}', 'YYYYMMDD')
        )
                AS st_int_dt,
        COALESCE(t.mature_dt, to_date('${MAXDATE}', 'YYYYMMDD'))
                AS mature_dt,
        CASE
        WHEN t.src_sys = 'S04_ACCT_CLOSED'
        AND t.agreement_stat_cd = 'XH'
        THEN t.close_dt
        ELSE to_date('${MAXDATE}', 'YYYYMMDD')
        END
                AS close_acct_dt,
        t9.agter_nm AS agter_nm,
        CASE
        WHEN t9.agter_ident_info_category_cd = 'CD_018'
        THEN t9.agter_ident_info_category_cd
        ELSE ''
        END
                AS agter_cert_type_cd,
        t9.agter_ident_info_content AS agter_cert_id,
        t9.agter_nationality_cd AS agter_nationality,
        t9.agter_tel AS agter_tel,
        t9.agt_open_acct_verify_situati
                AS agter_open_acct_verify_rslt,
        t.ccy_cd,
        t.open_acct_amt,
        COALESCE(substr(t5.tid, 1, 3), '') AS ftz_actype,
        CASE
        WHEN t5.tid IS NOT NULL OR t5.tid != '' THEN '1'
        ELSE '0'
        END
                AS ftz_act_ind,
        CASE
        WHEN t.category_cd = '4012' THEN 'D'
        ELSE COALESCE(t6.term_unit_cd, '')
        END
                AS term_type_cd,
        CASE
        WHEN t.category_cd = '4012'
        THEN to_number(substr(sub_prod_cd, 4, 1), '9')
        ELSE COALESCE(t6.term, 0)
        END
                AS deposit_periods,
        COALESCE(
                CASE
                WHEN (
                        (
                                (
                                        prod_cd = '1101'
                                        OR t.item_id
                                                IN ('14002', '15002', '16002')
                                )
                                OR COALESCE(
                                                t.sign_dt,
                                                to_date('${NULLDATE}', 'YYYYMMDD')
                                        )
                                        = to_date('${NULLDATE}', 'YYYYMMDD')
                        )
                        OR COALESCE(
                                        t.mature_dt,
                                        to_date('${MAXDATE}', 'YYYYMMDD')
                                )
                                = to_date('${NULLDATE}', 'YYYYMMDD')
                )
                OR COALESCE(
                                t.mature_dt,
                                to_date('${MAXDATE}', 'YYYYMMDD')
                        )
                        = to_date('${MAXDATE}', 'YYYYMMDD')
                THEN 0
                ELSE COALESCE(
                        t.mature_dt,
                        to_date('${MAXDATE}', 'YYYYMMDD')
                )
                - t.st_int_dt
                END,
                0
        )
                AS term_days,
        CASE
        WHEN prod_cd = '1101' THEN ''
        WHEN t.item_id
        IN (
                        '01015',
                        '01016',
                        '01017',
                        '099'
                )
        THEN 'M'
        WHEN t.item_id IN ('4002', '5002', '6002') THEN 'D'
        ELSE (
                CASE
                WHEN t6.term_unit_cd IS NOT NULL
                THEN t6.term_unit_cd
                ELSE (
                        CASE
                        WHEN term_days < 7 THEN ''
                        WHEN term_days >= 7 AND term_days < 28 THEN 'D'
                        ELSE 'M'
                        END
                )
                END
        )
        END
                AS adj_term_type_cd,
        CASE
        WHEN prod_cd = '1101' THEN 0
        WHEN t.item_id = '7216003' THEN 60
        WHEN t.item_id = '72111' THEN 3
        WHEN t.item_id
        IN ('7211014', '7211015', '7211016', '1099')
        THEN 12
        WHEN t.item_id = '7211017' THEN 24
        ELSE (
                CASE
                WHEN deposit_periods > 0 THEN deposit_periods
                ELSE (
                        CASE
                        WHEN term_days < 7 THEN 0
                        WHEN term_days >= 7 AND term_days < 28 THEN 7
                        WHEN term_days >= 28 AND term_days <= 31 THEN 1
                        WHEN term_days > 31 AND term_days <= 92 THEN 3
                        WHEN term_days > 92 AND term_days <= 184 THEN 6
                        WHEN term_days > 184 AND term_days <= 366
                        THEN 12
                        WHEN term_days > 366 AND term_days <= 731
                        THEN 24
                        WHEN term_days > 731 AND term_days <= 1096
                        THEN 36
                        WHEN term_days > 1096 THEN 60
                        END
                )
                END
        )
        END
                AS adj_deposit_periods,
        COALESCE(NULL, '') AS product_code,
        COALESCE(NULL, '') AS lmt_lnk_ind,
        COALESCE(t.cur_bal, 0.00) AS open_cleared_bal,
        t7.assoc_agreement_id AS limit_ref,
        COALESCE(t8.cash_pool_group, '') AS cash_pool_group,
        COALESCE(t10.medium_id, '') AS card_id
FROM
        agreement_item_temp AS t
        LEFT JOIN t03_acct AS t1 ON
                        t.agreement_id = t1.agreement_id
        LEFT JOIN t03_inform_dep_acct AS t3 ON
                        (
                                t3.agreement_id = t.agreement_id
                                AND t3.st_dt
                                        <= to_date('20011230', 'YYYYMMDD')
                        )
                        AND t3.end_dt > to_date('20011230', 'YYYYMMDD')
        LEFT JOIN t03_agreement_pty_rela_h_temp AS t4 ON
                        t4.agreement_id = t.agreement_id
        LEFT JOIN s04_zmq_acc_cur AS t5 ON
                        t5.customer = t.party_id
        LEFT JOIN acct_term_temp AS t6 ON t.agreement_id = t6.agreement_id
        LEFT JOIN t03_agreement_rela_h_temp AS t7 ON
                        t.agreement_id = t7.agreement_id
        LEFT JOIN agreement_cash_pool_temp AS t8 ON
                        t.agreement_id = t8.tid
        LEFT JOIN t03_agreement_agt_h AS t9 ON
                        (
                                t.agreement_id = t9.agreement_id
                                AND t9.st_dt
                                        <= to_date('20011230', 'YYYYMMDD')
                        )
                        AND t9.end_dt > to_date('20011230', 'YYYYMMDD')
        LEFT JOIN t03_agreement_medium_rela_h AS t10 ON
                        (
                                (
                                        t.agreement_id = t10.agreement_id
                                        AND t10.st_dt
                                                <= to_date('20011230', 'YYYYMMDD')
                                )
                                AND t10.end_dt
                                        > to_date('20011230', 'YYYYMMDD')
                        )
                        AND t10.agreement_medium_rela_type_cd = '2'
        LEFT JOIN t03_agreement_int_h AS t11 ON
                        (
                                (
                                        (
                                                t.agreement_id = t11.agreement_id
                                                AND t.agreement_mdfr = t11.agreement_mdfr
                                        )
                                        AND t11.st_dt
                                                <= to_date('20011230', 'YYYYMMDD')
                                )
                                AND t11.end_dt
                                        > to_date('20011230', 'YYYYMMDD')
                        )
                        AND t11.int_type_cd = '7'`

	SQL246Normal = `SELECT
	to_date('20011230', 'YYYYMMDD') AS data_dt,
	COALESCE(
		t03_agreement_pty_rela_h_temp.party_id,
		agreement_item_temp.sign_org
	)
		AS org_id,
	agreement_item_temp.agreement_id,
	t03_acct.fcy_spec_acct_id_type AS fcy_spec_acct_id_type,
	agreement_item_temp.agreement_mdfr,
	agreement_item_temp.category_cd,
	COALESCE(NULL, '') AS retail_ind,
	agreement_item_temp.item_id,
	CASE
	WHEN agreement_item_temp.category_cd IN ('1013', '1021')
	THEN '1101'
	WHEN agreement_item_temp.category_cd IN ('4009',)
	THEN '1102'
	WHEN agreement_item_temp.category_cd IN ('4013',)
	THEN '1201'
	WHEN agreement_item_temp.category_cd IN ('4010',)
	AND t03_inform_dep_acct.inform_deposit_category
		IN ('SLA', 'EDA')
	THEN '1202'
	WHEN agreement_item_temp.category_cd IN ('4012',)
	AND t03_inform_dep_acct.inform_deposit_category
		NOT IN ('NRI1', 'NRI7', 'IMM7', 'REN7')
	THEN '1301'
	WHEN agreement_item_temp.category_cd IN ('4012',)
	AND t03_inform_dep_acct.inform_deposit_category
		IN ('NRI1', 'NRI7')
	THEN '1302'
	WHEN agreement_item_temp.category_cd IN ('4012',)
	AND t03_inform_dep_acct.inform_deposit_category
		IN ('IMM7', 'REN7')
	THEN '1303'
	ELSE '9999'
	END
		AS prod_cd,
	t03_inform_dep_acct.inform_deposit_category
		AS sub_prod_cd,
	agreement_item_temp.party_id AS cust_id,
	agreement_item_temp.category_cd AS acct_category_cd,
	agreement_item_temp.agreement_category_cd
		AS acct_type_cd,
	CASE
	WHEN agreement_item_temp.category_cd = '1604' THEN '2'
	ELSE '1'
	END
		AS acct_stat_cd,
	CASE
	WHEN t03_acct.sleep_acct_ind = 'Y' THEN '1'
	ELSE '0'
	END
		AS dormancy_ind,
	CASE
	WHEN t03_acct.dep_exchg_ind = 'Y' THEN '1'
	ELSE '0'
	END
		AS dep_exchg_ind,
	COALESCE(t03_agreement_int_h.intr, 0.00) AS mature_intr,
	COALESCE(
		agreement_item_temp.sign_dt,
		to_date('${NULLDATE}', 'YYYYMMDD')
	)
		AS open_acct_dt,
	COALESCE(
		agreement_item_temp.st_int_dt,
		to_date('${NULLDATE}', 'YYYYMMDD')
	)
		AS st_int_dt,
	COALESCE(
		agreement_item_temp.mature_dt,
		to_date('${MAXDATE}', 'YYYYMMDD')
	)
		AS mature_dt,
	CASE
	WHEN agreement_item_temp.src_sys = 'S04_ACCT_CLOSED'
	AND agreement_item_temp.agreement_stat_cd = 'XH'
	THEN agreement_item_temp.close_dt
	ELSE to_date('${MAXDATE}', 'YYYYMMDD')
	END
		AS close_acct_dt,
	t03_agreement_agt_h.agter_nm AS agter_nm,
	CASE
	WHEN t03_agreement_agt_h.agter_ident_info_category_cd
	= 'CD_018'
	THEN t03_agreement_agt_h.agter_ident_info_category_cd
	ELSE ''
	END
		AS agter_cert_type_cd,
	t03_agreement_agt_h.agter_ident_info_content
		AS agter_cert_id,
	t03_agreement_agt_h.agter_nationality_cd
		AS agter_nationality,
	t03_agreement_agt_h.agter_tel AS agter_tel,
	t03_agreement_agt_h.agt_open_acct_verify_situati
		AS agter_open_acct_verify_rslt,
	agreement_item_temp.ccy_cd,
	agreement_item_temp.open_acct_amt,
	COALESCE(substr(s04_zmq_acc_cur.tid, 1, 3), '')
		AS ftz_actype,
	CASE
	WHEN s04_zmq_acc_cur.tid IS NOT NULL
	OR s04_zmq_acc_cur.tid != ''
	THEN '1'
	ELSE '0'
	END
		AS ftz_act_ind,
	CASE
	WHEN agreement_item_temp.category_cd = '4012' THEN 'D'
	ELSE COALESCE(acct_term_temp.term_unit_cd, '')
	END
		AS term_type_cd,
	CASE
	WHEN agreement_item_temp.category_cd = '4012'
	THEN to_number(
		substr(
			t03_inform_dep_acct.inform_deposit_category,
			4,
			1
		),
		'9'
	)
	ELSE COALESCE(acct_term_temp.term, 0)
	END
		AS deposit_periods,
	COALESCE(
		CASE
		WHEN (
			(
				(
					prod_cd = '1101'
					OR agreement_item_temp.item_id
						IN ('14002', '15002', '16002')
				)
				OR COALESCE(
						agreement_item_temp.sign_dt,
						to_date('${NULLDATE}', 'YYYYMMDD')
					)
					= to_date('${NULLDATE}', 'YYYYMMDD')
			)
			OR COALESCE(
					agreement_item_temp.mature_dt,
					to_date('${MAXDATE}', 'YYYYMMDD')
				)
				= to_date('${NULLDATE}', 'YYYYMMDD')
		)
		OR COALESCE(
				agreement_item_temp.mature_dt,
				to_date('${MAXDATE}', 'YYYYMMDD')
			)
			= to_date('${MAXDATE}', 'YYYYMMDD')
		THEN 0
		ELSE COALESCE(
			agreement_item_temp.mature_dt,
			to_date('${MAXDATE}', 'YYYYMMDD')
		)
		- agreement_item_temp.st_int_dt
		END,
		0
	)
		AS term_days,
	CASE
	WHEN prod_cd = '1101' THEN ''
	WHEN agreement_item_temp.item_id
	IN ('01015', '01016', '01017', '099')
	THEN 'M'
	WHEN agreement_item_temp.item_id
	IN ('4002', '5002', '6002')
	THEN 'D'
	ELSE (
		CASE
		WHEN acct_term_temp.term_unit_cd IS NOT NULL
		THEN acct_term_temp.term_unit_cd
		ELSE (
			CASE
			WHEN term_days < 7 THEN ''
			WHEN term_days >= 7 AND term_days < 28 THEN 'D'
			ELSE 'M'
			END
		)
		END
	)
	END
		AS adj_term_type_cd,
	CASE
	WHEN prod_cd = '1101' THEN 0
	WHEN agreement_item_temp.item_id = '7216003' THEN 60
	WHEN agreement_item_temp.item_id = '72111' THEN 3
	WHEN agreement_item_temp.item_id
	IN ('7211014', '7211015', '7211016', '1099')
	THEN 12
	WHEN agreement_item_temp.item_id = '7211017' THEN 24
	ELSE (
		CASE
		WHEN deposit_periods > 0 THEN deposit_periods
		ELSE (
			CASE
			WHEN term_days < 7 THEN 0
			WHEN term_days >= 7 AND term_days < 28 THEN 7
			WHEN term_days >= 28 AND term_days <= 31 THEN 1
			WHEN term_days > 31 AND term_days <= 92 THEN 3
			WHEN term_days > 92 AND term_days <= 184 THEN 6
			WHEN term_days > 184 AND term_days <= 366
			THEN 12
			WHEN term_days > 366 AND term_days <= 731
			THEN 24
			WHEN term_days > 731 AND term_days <= 1096
			THEN 36
			WHEN term_days > 1096 THEN 60
			END
		)
		END
	)
	END
		AS adj_deposit_periods,
	COALESCE(NULL, '') AS product_code,
	COALESCE(NULL, '') AS lmt_lnk_ind,
	COALESCE(agreement_item_temp.cur_bal, 0.00)
		AS open_cleared_bal,
	t03_agreement_rela_h_temp.assoc_agreement_id
		AS limit_ref,
	COALESCE(agreement_cash_pool_temp.cash_pool_group, '')
		AS cash_pool_group,
	COALESCE(t03_agreement_medium_rela_h.medium_id, '')
		AS card_id
FROM
	agreement_item_temp
	LEFT JOIN t03_acct ON
			agreement_item_temp.agreement_id
			= t03_acct.agreement_id
	LEFT JOIN t03_inform_dep_acct ON
			(
				t03_inform_dep_acct.agreement_id
				= agreement_item_temp.agreement_id
				AND t03_inform_dep_acct.st_dt
					<= to_date('20011230', 'YYYYMMDD')
			)
			AND t03_inform_dep_acct.end_dt
				> to_date('20011230', 'YYYYMMDD')
	LEFT JOIN t03_agreement_pty_rela_h_temp ON
			t03_agreement_pty_rela_h_temp.agreement_id
			= agreement_item_temp.agreement_id
	LEFT JOIN s04_zmq_acc_cur ON
			s04_zmq_acc_cur.customer
			= agreement_item_temp.party_id
	LEFT JOIN acct_term_temp ON
			agreement_item_temp.agreement_id
			= acct_term_temp.agreement_id
	LEFT JOIN t03_agreement_rela_h_temp ON
			agreement_item_temp.agreement_id
			= t03_agreement_rela_h_temp.agreement_id
	LEFT JOIN agreement_cash_pool_temp ON
			agreement_item_temp.agreement_id
			= agreement_cash_pool_temp.tid
	LEFT JOIN t03_agreement_agt_h ON
			(
				agreement_item_temp.agreement_id
				= t03_agreement_agt_h.agreement_id
				AND t03_agreement_agt_h.st_dt
					<= to_date('20011230', 'YYYYMMDD')
			)
			AND t03_agreement_agt_h.end_dt
				> to_date('20011230', 'YYYYMMDD')
	LEFT JOIN t03_agreement_medium_rela_h ON
			(
				(
					agreement_item_temp.agreement_id
					= t03_agreement_medium_rela_h.agreement_id
					AND t03_agreement_medium_rela_h.st_dt
						<= to_date('20011230', 'YYYYMMDD')
				)
				AND t03_agreement_medium_rela_h.end_dt
					> to_date('20011230', 'YYYYMMDD')
			)
			AND t03_agreement_medium_rela_h.agreement_medium_rela_type_cd
				= '2'
	LEFT JOIN t03_agreement_int_h ON
			(
				(
					(
						agreement_item_temp.agreement_id
						= t03_agreement_int_h.agreement_id
						AND agreement_item_temp.agreement_mdfr
							= t03_agreement_int_h.agreement_mdfr
					)
					AND t03_agreement_int_h.st_dt
						<= to_date('20011230', 'YYYYMMDD')
				)
				AND t03_agreement_int_h.end_dt
					> to_date('20011230', 'YYYYMMDD')
			)
			AND t03_agreement_int_h.int_type_cd = '7'`

	SQLSimpleJoin = `SELECT t1.id, 1 + 2 + t1.value AS v FROM t1 JOIN t2 ON t1.id = t2.id AND t2.id -1 > 50 * 1000`

	SQLPredicatePushDown = `
SELECT sum(v) sumv
	FROM (
		SELECT
			t1.id,
			1 + 2 + t1.value AS v
		FROM t1 JOIN t2
		ON t1.id = t2.id AND
			t2.id > 50 * 1000) tmp
	`

	//FROM https://www.geekytidbits.com/postgres-distinct-on/
	SQLURLLog = `SELECT l.url, l.request_duration
FROM log l
INNER JOIN (
  SELECT url, MAX(timestamp) as max_timestamp
  FROM log
  GROUP BY url
) last_by_url ON l.url = last_by_url.url AND l.timestamp = last_by_url.max_timestamp;`

	SQLSelectInProjection = `select (select CERT_NO from COMR_CIFBINFO) e;`

	SQLJoinCondition1 = `
		SELECT i.id AS id,
		   i.shopid AS sid,
		   i.name AS name,
		   i.brand AS brand,
		   s.shop_status AS shop_status,
           t.domestic AS domestic
		FROM item i
		LEFT JOIN shop s 
			ON i.id = s.shopid
		LEFT JOIN tag t 
			ON t.itemid = id AND t.shopid = s.shopid
	`

	SQLJoinCondition2 = `
		SELECT i.id AS id,
		   i.shopid AS sid,
		   i.name AS name,
		   i.brand AS brand,
		   s.shop_status AS shop_status,
           t.domestic AS domestic
		FROM item i
		LEFT JOIN 
		(SELECT shop_status, shopid FROM shop) s
			ON s.shopid = i.id
		LEFT JOIN tag t 
			USING (itemid)
	`

	SQLSimpleLeftJoin = `
		SELECT i.itemid AS id,
		   i.shopid AS sid,
		   i.name AS name,
		   i.brand AS brand,
		   s.shop_status AS shop_status
		FROM item i
		LEFT JOIN shop s 
			ON i.shopid = s.shopid`

	SQLLeftJoin = `
		SELECT i.itemid AS id,
		   i.shopid AS sid,
		   i.name AS name,
		   i.brand AS brand,
		   s.shop_status AS shop_status,
		   t.domestic AS domestic,
		   ip.discount AS discount,
		   p.gender AS gender,
		   p.age AS age
		FROM item i
		LEFT JOIN shop s 
			ON i.shopid = s.shopid
		LEFT JOIN tag t 
			ON i.itemid = t.itemid AND i.shopid = t.shopid
		LEFT JOIN price ip 
			ON i.itemid = ip.itemid
		LEFT JOIN item_prof p 
			ON i.itemid = p.itemid`
)
