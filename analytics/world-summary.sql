select
	A.confirmed,
	B.confirmed_last_week,
	((A.confirmed - B.confirmed_last_week)::float / B.confirmed_last_week) * 100 confirmed_increased,
	C.recovered,
	C.recovered_last_week,
	((C.recovered - C.recovered_last_week)::float / C.recovered_last_week) * 100 recovered_increased,
	A.death,
	B.death_last_week,
	((A.death - B.death_last_week)::float / B.death_last_week) * 100 death_increased
from
	(
	select
		1 as id,
		(sum(total_cases) + sum(new_cases)) confirmed,
		(sum(total_deaths) + sum(new_deaths)) death
	from
		owd_data
	where
		date = (
		select
			max(date)
		from
			owd_data)) A
inner join (
	select
		1 as id,
		(sum(total_cases) + sum(new_cases)) confirmed_last_week,
		(sum(total_deaths) + sum(new_deaths)) death_last_week
	from
		owd_data
	where
		date = (
		select
			max(date) - 7
		from
			owd_data)) B on
	A.id = B.id
inner join (
	select
		C.id,
		recovered,
		recovered_last_week
	from
		(
		select
			1 as id,
			recovered
		from
			worldwide_aggregated
		where
			date = (
			select
				max(date)
			from
				worldwide_aggregated)) C
	join (
		select
			1 as id,
			recovered as recovered_last_week
		from
			worldwide_aggregated
		where
			date = (
			select
				max(date) - 7
			from
				worldwide_aggregated) ) D on
		C.id = D.id ) C on
	A.id = C.id