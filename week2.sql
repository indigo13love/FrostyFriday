-- Create a file format and an internal stage
create or replace file format parquet type = parquet;
create or replace stage week2 file_format = parquet;

/***** via SnowSQL ****/

-- Load the parquet file to the stage
put file://~/Downloads/employees.parquet @week2;
/*
+-------------------+-------------------+-------------+-------------+--------------------+--------------------+----------+---------+
| source            | target            | source_size | target_size | source_compression | target_compression | status   | message |
|-------------------+-------------------+-------------+-------------+--------------------+--------------------+----------+---------|
| employees.parquet | employees.parquet |       22467 |       22480 | PARQUET            | PARQUET            | UPLOADED |         |
+-------------------+-------------------+-------------+-------------+--------------------+--------------------+----------+---------+
1 Row(s) produced. Time Elapsed: 1.655s
*/

/**********************/

-- Infer the parquet file schema
select *
from table(infer_schema(location => '@week2', file_format => 'parquet'));
/*
COLUMN_NAME	TYPE	NULLABLE	EXPRESSION	FILENAMES	ORDER_ID
employee_id	NUMBER(38, 0)	TRUE	$1:employee_id::NUMBER(38, 0)	employees.parquet	0
first_name	TEXT	TRUE	$1:first_name::TEXT	employees.parquet	1
last_name	TEXT	TRUE	$1:last_name::TEXT	employees.parquet	2
email	TEXT	TRUE	$1:email::TEXT	employees.parquet	3
street_num	NUMBER(38, 0)	TRUE	$1:street_num::NUMBER(38, 0)	employees.parquet	4
street_name	TEXT	TRUE	$1:street_name::TEXT	employees.parquet	5
city	TEXT	TRUE	$1:city::TEXT	employees.parquet	6
postcode	TEXT	TRUE	$1:postcode::TEXT	employees.parquet	7
country	TEXT	TRUE	$1:country::TEXT	employees.parquet	8
country_code	TEXT	TRUE	$1:country_code::TEXT	employees.parquet	9
time_zone	TEXT	TRUE	$1:time_zone::TEXT	employees.parquet	10
payroll_iban	TEXT	TRUE	$1:payroll_iban::TEXT	employees.parquet	11
dept	TEXT	TRUE	$1:dept::TEXT	employees.parquet	12
job_title	TEXT	TRUE	$1:job_title::TEXT	employees.parquet	13
education	TEXT	TRUE	$1:education::TEXT	employees.parquet	14
title	TEXT	TRUE	$1:title::TEXT	employees.parquet	15
suffix	TEXT	TRUE	$1:suffix::TEXT	employees.parquet	16
*/

-- Build CREATE TABLE from INFER_SCHEMA
select 'create or replace table week2 (' || listagg(column_name || ' ' || type, ', ') || ') as select ' || listagg(expression, ', ') || ' from week2_tmp;'
from table(infer_schema(location => '@week2', file_format => 'parquet'));
-- create or replace table week2 (employee_id NUMBER(38, 0), first_name TEXT, last_name TEXT, email TEXT, street_num NUMBER(38, 0), street_name TEXT, city TEXT, postcode TEXT, country TEXT, country_code TEXT, time_zone TEXT, payroll_iban TEXT, dept TEXT, job_title TEXT, education TEXT, title TEXT, suffix TEXT) as select $1:employee_id::NUMBER(38, 0), $1:first_name::TEXT, $1:last_name::TEXT, $1:email::TEXT, $1:street_num::NUMBER(38, 0), $1:street_name::TEXT, $1:city::TEXT, $1:postcode::TEXT, $1:country::TEXT, $1:country_code::TEXT, $1:time_zone::TEXT, $1:payroll_iban::TEXT, $1:dept::TEXT, $1:job_title::TEXT, $1:education::TEXT, $1:title::TEXT, $1:suffix::TEXT from week2_tmp;

-- Load the parquet file from the stage to a table
create or replace table week2_tmp (v variant);
copy into week2_tmp from @week2 file_format = parquet;

-- Columnize the loaded variant
create or replace table week2 (
    employee_id NUMBER(38, 0), first_name TEXT, last_name TEXT, email TEXT, street_num NUMBER(38, 0),
    street_name TEXT, city TEXT, postcode TEXT, country TEXT, country_code TEXT, time_zone TEXT,
    payroll_iban TEXT, dept TEXT, job_title TEXT, education TEXT, title TEXT, suffix TEXT
) as
select
    $1:employee_id::NUMBER(38, 0), $1:first_name::TEXT, $1:last_name::TEXT, $1:email::TEXT, $1:street_num::NUMBER(38, 0),
    $1:street_name::TEXT, $1:city::TEXT, $1:postcode::TEXT, $1:country::TEXT, $1:country_code::TEXT, $1:time_zone::TEXT,
    $1:payroll_iban::TEXT, $1:dept::TEXT, $1:job_title::TEXT, $1:education::TEXT, $1:title::TEXT, $1:suffix::TEXT
from week2_tmp;

select * from week2;

-- Create a view to limit the columns to be tracked
create or replace view week2_view as
select employee_id, dept, job_title from week2;

-- Create a stream
create or replace stream week2_stream on view week2_view;

-- Run the specified DMLs
UPDATE week2 SET COUNTRY = 'Japan' WHERE EMPLOYEE_ID = 8;
UPDATE week2 SET LAST_NAME = 'Forester' WHERE EMPLOYEE_ID = 22;
UPDATE week2 SET DEPT = 'Marketing' WHERE EMPLOYEE_ID = 25;
UPDATE week2 SET TITLE = 'Ms' WHERE EMPLOYEE_ID = 32;
UPDATE week2 SET JOB_TITLE = 'Senior Financial Analyst' WHERE EMPLOYEE_ID = 68;

-- Check the stream
select employee_id, dept, job_title, metadata$row_id, metadata$action, metadata$isupdate
from week2_stream;
/*
EMPLOYEE_ID	DEPT	JOB_TITLE	METADATA$ROW_ID	METADATA$ACTION	METADATA$ISUPDATE
68	Product Management	Senior Financial Analyst	0570b8d53d08a4094f9ee3a988f1a82aa3a6ecd5	INSERT	TRUE
68	Product Management	Assistant Manager	0570b8d53d08a4094f9ee3a988f1a82aa3a6ecd5	DELETE	TRUE
25	Marketing	Assistant Professor	a35cd36a178b691843a8e5d9da8585ff581f47c6	INSERT	TRUE
25	Accounting	Assistant Professor	a35cd36a178b691843a8e5d9da8585ff581f47c6	DELETE	TRUE
*/
