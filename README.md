# go-sql-lineage
Pure golang SQL lineage analysis toolkit

1. SQL:2011 based on [postgresql-parser](https://github.com/auxten/postgresql-parser)
1. Very complex SQL tested, see [SQLs](test/sql/sqls.go), 65.7% test coverage 

## Quick Start

1. install:
```shell
go install github.com/auxten/go-sql-lineage@latest
```

2. run:
```shell
go-sql-lineage -schema '[{"Name":"a","Cols":[{"Name":"id"},{"Name":"oper_no"},
    {"Name":"oper_name"}]},{"Name":"b","Cols":[{"Name":"id"},{"Name":"cert_no"}
        ,{"Name":"cust_no"}]}]' << EOF
select oper_name, cert_no from a join b on a.id = b.id
EOF
```

3. got output:
```
Normalized SQL: SELECT a.oper_name, b.cert_no FROM a JOIN b ON a.id = b.id
SQL Refered Columns: [a.oper_name:Physical b.cert_no:Physical a.id:Physical b.id:Physical]
SQL Input Tables: [a b]
SQL Input Columns: [a.id:Physical a.oper_no:Physical a.oper_name:Physical b.id:Physical b.cert_no:Physical b.cust_no:Physical]
SQL Output Columns: [a.oper_name:Physical b.cert_no:Physical]
```

<details>
  <summary>Complex Cmd Example</summary>

Input example:
```shell
go-sql-lineage -schema '[{"Name":"COMC_CLERK","Cols":[{"Name":"rowid"},{"Name":"oper_no"},{"Name":"oper_name"},{"Name":"cert_no"},{"Name":"oper_no"}]},{"Name":"comr_cifbinfo","Cols":[{"Name":"rowid"},{"Name":"cert_no"},{"Name":"cust_no"}]},{"Name":"savb_basicinfo","Cols":[{"Name":"rowid"},{"Name":"cust_no"},{"Name":"acct_no"},{"Name":"unnecessary"}]},{"Name":"savb_acctinfo_chk","Cols":[{"Name":"rowid"},{"Name":"sub_code"},{"Name":"acct_no"},{"Name":"acct_bal"}]}]' << EOF
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
EOF
```

Output example:
```sql
Normalized SQL: SELECT
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
    1000
SQL Refered Columns: [comc_clerk.oper_no:Physical comc_clerk.oper_name:Physical comc_clerk.cert_no:Physical acct.acct_bal:Physical comc_clerk.oper_no:Physical comc_clerk.oper_no:Physical sum_bal:Logical comc_clerk.oper_no:Physical comc_clerk.oper_no:Physical comc_clerk.oper_name:Physical comc_clerk.oper_name:Physical comc_clerk.cert_no:Physical comc_clerk.cert_no:Physical comr_cifbinfo.cert_no:Physical savb_basicinfo.cust_no:Physical savb_basicinfo.acct_no:Physical savb_basicinfo.unnecessary:Physical comr_cifbinfo.cust_no:Physical basic.cust_no:Logical savb_acctinfo_chk.sub_code:Physical savb_acctinfo_chk.acct_no:Physical savb_acctinfo_chk.acct_bal:Physical basic.acct_no:Logical acct.acct_no:Logical acct.sub_code:Physical]
SQL Input Tables: [comc_clerk comr_cifbinfo basic acct]
SQL Input Columns: [comc_clerk.rowid:Physical comc_clerk.oper_no:Physical comc_clerk.oper_name:Physical comc_clerk.cert_no:Physical comc_clerk.oper_no:Physical comr_cifbinfo.rowid:Physical comr_cifbinfo.cert_no:Physical comr_cifbinfo.cust_no:Physical basic.cust_no:Physical basic.acct_no:Physical basic.unnecessary:Physical acct.sub_code:Physical acct.acct_no:Physical acct.acct_bal:Physical]
SQL Output Columns: [comc_clerk.oper_no:Physical comc_clerk.oper_name:Physical comc_clerk.cert_no:Physical sum_bal:Unknown]
```
</details>


## SDK Usage

### Normalize SQL

Normalize SQL will:

1. Rewrite any implicit column reference to explicit column reference with best effort.
2. Remove comments
3. Re-indent SQL

Example:

```sql
SELECT a, b, c
FROM
    t1
WHERE a = 1 AND b = 2 AND c = 3
```

will be normalized to:

```sql
SELECT
    t1.a, t1.b, t1.c
FROM
    t1
WHERE
    t1.a = 1 AND t1.b = 2 AND t1.c = 3
```

for better inference, input tables schema will be VERY helpful.

<details>
  <summary>Complex SQL Example</summary>

Input example:
```sql
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
      GROUP BY JOB_ID) SS) ;
```

Output example:
```sql
SELECT
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
        )
```
</details>



### Get SQL input/output tables and columns

1. `GetInputColumns` will return all input columns of a SQL statement.
1. `GetOutputColumns` will return all output columns of a SQL statement.

Example could be found in [from_test.go](lineage/from_test.go).

## Known issue

1. star(*) is not supported yet.
2. CTE is not supported yet.