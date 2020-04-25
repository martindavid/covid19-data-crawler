with base_data as (
	SELECT A.date,
	          confirmed,
	          coalesce(LAG(confirmed , 1) OVER (
	              ORDER BY A.date
	              ), 0) previous_day_confirmed,
	          death,
	          coalesce(LAG(death, 1) OVER (
	              ORDER BY A.date
	              ), 0) previous_day_death,
	          case 
	          	when B.recovered is null then 0 else B.recovered end as recovered,
	          case 
	            when B.previous_day_recovered is null then 0 else B.previous_day_recovered end as previous_day_recovered
	  FROM (
	  select
		od.date,
		sum(od.new_cases) as confirmed,
		sum(new_deaths) as death
	from
		owd_data od
	where od.date between '2020-01-22' and now()::date - 1
	group by
		od.date
	  ) A
	left join (
	  SELECT date,
	          recovered,
	          coalesce(LAG(recovered, 1) OVER (
	              ORDER BY date
	              ), 0) previous_day_recovered
	  FROM worldwide_aggregated
	) B on A.date = B.date
)

select TO_CHAR(date, 'YYYY-MM-DD') date,
        confirmed,
        case
            when previous_day_confirmed = 0 then 0
            else confirmed - previous_day_confirmed end as daily_confirmed_rate_num,
        case
            when previous_day_confirmed = 0 then 0
            else ((confirmed - previous_day_confirmed)::float / previous_day_confirmed) end as daily_confirmed_rate,
        recovered,
        case
            when previous_day_recovered = 0 then 0
            else recovered - previous_day_recovered end as daily_recovered_rate_num,
        case
            when previous_day_recovered = 0 then 0
            else (recovered - previous_day_recovered) end as recovered_increment_rate_num,
        death,
        case
            when previous_day_death = 0 then 0
            else death - previous_day_death end as daily_death_rate_num,
        case
            when previous_day_death = 0 then 0
            else ((death - previous_day_death)::float / previous_day_death) end as daily_death_rate
  from base_data A;