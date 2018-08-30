select *
from payment;

select sum(amount), sum(payment_date)
from payment;

select to_char(payment_date,'yyyy/mm'), sum(amount)
from payment
group by to_char(payment_date,'yyyy/mm') --, rollup (to_char(payment_date,'yyyy/mm'))
order by 1;

select  sum(amount) over (partition by to_char(payment_date,'yyyy/mm') order by payment_date), t.*
from payment t
order by payment_date;

with q
as (
	select to_char(payment_date,'yyyy/mm') mon, sum(amount) mon_amount
	from payment
	group by to_char(payment_date,'yyyy/mm') 
	order by 1
)
select mon, mon_amount, sum(mon_amount) over (order by mon) accum_total
from q t;

select 	sum(amount) over(partition by to_char(payment_date,'yyyy/mm')) monthly_tot
		,sum(amount) over(partition by to_char(payment_date,'yyyy/mm') order by payment_date) as running_tot, t.*
from payment t
order by payment_date;


