-- 第一题答案
-- 连续值的求解，基本都可按照以下思路进行
-- 1、使用 row_number 在组内给数据编号(rownum)
-- 2、某个值 - rownum = gid，得到结果可以作为后面分组计算的依据
-- 3、根据求得的gid，作为分组条件，求最终结果
-- 解题思路：1、本题就以year为依据进行求差计算。
 
SELECT
a.team,count( 1 ) AS duoguan 
FROM
( SELECT team, YEAR, ( YEAR - row_number ( ) over ( PARTITION BY team ORDER BY YEAR ) ) AS rn FROM t1 ) a 
GROUP BY
a.team,a.rn 
HAVING
duoguan >= 3;
	
	
-- 第二题答案
--解题思路：1、首先需要定义波峰和波谷的含义：price 和前一时间点的price差值和后一时间点的price差值都为正数则为波峰，反之为波谷。
--2、处于波峰和波谷之间的数据肯定是price和前后的price差值一个为正一个为负。
--3、利用case when语句进行判断即可
select b.* 
from (select a.id,a.time,a.price,
case when a.r1>=0 and a.r2>=0 then '波峰'
when a.r1<0 and a.r2<0 then '波谷'
else 'else' end as status from 
(SELECT id, 
time, 
price, 
round(price - nvl(lag( price ) over ( PARTITION BY id ORDER BY time ),price),2) as r1,
round(price - nvl(lead ( price ) over ( PARTITION BY id ORDER BY time ),price),2) AS r2 FROM t2 ) a) b 
where b.status <>'else';


-- 第三题答案
--3.1 解题思路 1、时间相减得出的差值为秒，除以60得到分钟。这个时间格式不包含秒，所以不需要用到round 函数，正常时间格式的话可能会用到round函数
--2、利用窗口函数进行排序就OK了
select id,sum(staytime) toal_staytime,max(rank) total_step 
  from (select id, dt,browseid,row_number() over (partition by id order by dt) rank,
    (unix_timestamp(dt, 'yyyy/MM/dd HH:mm') - unix_timestamp(nvl(lag(dt) over (partition by id order by dt),dt), 'yyyy/MM/dd HH:mm'))/60 as staytime from t3) tmp1 
group by id;

-- 3.2
--解题思路 1、由于设置限定条件，时间间隔超过30分钟即需要重新计算；需要设计一个字段来区分同一个用户不同次的标记。
--2、浏览时长和3.1类似。
--3、利用sum的窗口函数可以将重新浏览后的记录标记都表为1；如何还有再一次的重新计算，标记都标记为2.以此类推，
--4、然后根据用户id和标记进行分组计算即可
select id,sum(staytime) total_staytime, count(*)  logcount from(
    select id ,dt, recount_flag,sum(recount_flag) over(partition by id order by dt) recount_flags,staytime from (
    select id,dt,
(unix_timestamp(dt,"yyyy/mm/dd hh:mm")-unix_timestamp(lag(dt,1,dt) over(partition by id order by dt) ,"yyyy/mm/dd hh:mm"))/60 staytime,
case when (unix_timestamp(dt,"yyyy/mm/dd hh:mm")-
unix_timestamp(lag(dt,1,dt) over(partition by id order by dt) ,"yyyy/mm/dd hh:mm"))/60 >30 then 1
else 0 end as recount_flag
from t3
) b
) c group  by  id,recount_flags;
