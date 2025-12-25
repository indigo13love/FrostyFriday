-- たぶん一番人気の解法
select iff((seq4()+1)%15=0, 'FizzBuzz', iff((seq4()+1)%5=0, 'Buzz', iff((seq4()+1)%3=0, 'Fizz', (seq4()+1)::varchar)))
from table(generator(rowcount => 100));

-- 個人的に SQL っぽい (しかも行数増えると速い) 解法
with
seq as (select seq4()+1 n from table(generator(rowcount => 100))),
fizz as (select n, 'Fizz' v from seq where n%3 = 0),
buzz as (select n, 'Buzz' v from seq where n%5 = 0),
fizzbuzz as (select n, 'FizzBuzz' v from seq where n%15 = 0)
select coalesce(fb.v, b.v, f.v, s.n::varchar)
from seq s
left join fizz f on s.n = f.n
left join buzz b on s.n = b.n
left join fizzbuzz fb on s.n = fb.n
order by s.n
;

-- 個人的に好きな解法
with
seq as (select seq4()+1 n from table(generator(rowcount => 10000000))),
fizzbuzz (n, v) as (
    select *
    from (values
        (1, null),
        (2, null),
        (3, 'Fizz'),
        (4, null),
        (5, 'Buzz'),
        (6, 'Fizz'),
        (7, null),
        (8, null),
        (9, 'Fizz'),
        (10, 'Buzz'),
        (11, null),
        (12, 'Fizz'),
        (13, null),
        (14, null),
        (0, 'FizzBuzz')
    )
)
select coalesce(fb.v, fb.n::varchar)
from seq s
join fizzbuzz fb on (s.n%15) = fb.n
order by s.n
;
