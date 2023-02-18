create or replace stage week3 url = 's3://frostyfridaychallenges/challenge_3/';

list @week3;

select $1 from @week3/keywords.csv;
create or replace file format csv_with_header type = csv, skip_header = 1;

list @week3;
set qid = last_query_id();

select *
from table(result_scan($qid))
where "name" like any (select '%' || $1 || '%' from @week3/keywords.csv (file_format => csv_with_header))
;
/*
name	size	md5	last_modified
s3://frostyfridaychallenges/challenge_3/week3_data2_stacy_forgot_to_upload.csv	732	62dd3f36fe5ac5f06ec23fbc74075256	Tue, 12 Jul 2022 18:08:12 GMT
s3://frostyfridaychallenges/challenge_3/week3_data4_extra.csv	773	3f4a5f0ef3f11e2be542a5cea2cb854e	Tue, 12 Jul 2022 18:08:13 GMT
s3://frostyfridaychallenges/challenge_3/week3_data5_added.csv	848	087dbd8bb7f867372087737234961221	Tue, 12 Jul 2022 18:08:14 GMT
*/
