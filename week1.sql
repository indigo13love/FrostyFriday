create or replace stage week1 url = 's3://frostyfridaychallenges/challenge_1/';

list @week1;
select $1 from @week1;

create or replace table week1 (c1 varchar);
copy into week1 from @week1 file_format = (type = csv);
/*
file	status	rows_parsed	rows_loaded	error_limit	errors_seen	first_error	first_error_line	first_error_character	first_error_column_name
s3://frostyfridaychallenges/challenge_1/2.csv	LOADED	2	2	1	0				
s3://frostyfridaychallenges/challenge_1/1.csv	LOADED	4	4	1	0				
s3://frostyfridaychallenges/challenge_1/3.csv	LOADED	5	5	1	0				
*/

select * from week1;
/*
C1
result
it
result
you
have
gotten
result
right
NULL
totally_empty
congratulations!
*/
