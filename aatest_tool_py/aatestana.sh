sday='2019-08-01'
eday='2019-08-14'
hive -e"
add jar hdfs://datalake/user/hdp-data/dw-udf/dw-udf-jar-with-md5.jar;
create temporary function md5 as 'com.tantanapp.udf.Md5';
set hive.cli.print.header=true;
set hive.resultset.use.unique.column.names=false;
set hive.execution.engine=mr;
set mapreduce.reduce.memory.mb=8192;
set mapred.reduce.tasks=20;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.mapred.mode=nonstrict;
set hive.exec.max.dynamic.partitions.pernode=1000;
set hive.exec.max.dynamic.partitions=2000;


select
    a.dt
    ,a.user_id
    ,a.gender
    ,a.os
    ,b.revenue
    ,b.swipe
    ,b.likes
    ,b.match
    ,b.messages 
from 
(
    select
        dt
        ,user_id
        ,int(pmod(conv(substr(md5(concat(user_id ,10000019)),1,6),16,10),1000)) type
        ,latest_city_name_zh city
        ,gender
        ,latest_os_name os 
    from 
        dwd.dwd_daily_active_users_info_wide_table
    where
        dt between '$sday' and '$eday'
        and status='default'
        and latest_country_name_zh='中国'
        and latest_city_name_zh is not null
) a 
join
(
    select 
        dt
        ,user_id
        ,swipe
        ,likes
        ,messages
        ,match
        ,revenue
    from 
        dws.dws_user_metrics_i_d
    where 
		dt between '$sday' and '$eday'
		and swipe<=20000 
		and likes<=20000 
		and dislike<=10000 
		and match<=2000
		and messages<=2000 
		and new_messages<=1500 
		and one_side_conversation<=500 
		and new_one_side_conversation<=300 
		and one_mutual_msg_conversation<=400 
		and new_one_mutual_msg_conversation<=300 
		and five_mutual_msg_conversation<=100
		and new_five_mutual_msg_conversation<=100
		and ten_mutual_msg_conversation <=50
		and new_ten_mutual_msg_conversation<=50
		and twenty_mutual_msg_conversation<=30
		and new_twenty_mutual_msg_conversation<=30
		and moment_post<=100
		and followship_moment_send<=2000
		and nearby_moment_send<=10000
		and followship_moment_read<=1000
		and nearby_moment_read<=2000
		and moment_comment<=150
		and moment_like<=300
		and follow<=250
) b

on a.dt=b.dt and a.user_id=b.user_id 






;
"|sed 's/[\t]/,/g'>aatestana.csv