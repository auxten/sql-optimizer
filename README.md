# go-sql-lineage
Pure golang SQL lineage analyzer


## Quick Start

1. install:
```bash
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