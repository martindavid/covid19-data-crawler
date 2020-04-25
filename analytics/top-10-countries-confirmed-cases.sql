with days as (
    select generate_series(timestamp '2020-01-22', now()::date, '1 day')::date AS day
)

select
      B.day as date,
      B.country,
      case
	      when A.total_cases is null then 0
	      else A.total_cases end as confirmed
from owd_data A
right join (select B.day, A.country
    from (
            select
				location as country,
				total_cases as confirmed
			from
				owd_data od
			where
				date =(
				select
					max(date)
				from
					owd_data)
			order by
				total_cases desc
			limit 10
        ) A
            cross join days B) B on A.location = B.country and A.date = B.day;