select
	A.country,
	A.confirmed,
	A.death,
	B.recovered,
	A.iso3
from
	(
	select
		iso_code as iso3,
		location as country,
		total_cases as confirmed,
		total_deaths as death
	from
		owd_data od
	where
		date = (
		select
			max(date)
		from
			owd_data) ) A
join (
	select
		country,
		recovered
		iso3
	from
		country_aggregated A
	left join country B on
		A.country = B.nicename
	where
		date = (
		select
			max(date)
		from
			country_aggregated)
	order by
		A.country) B on
	A.iso3 = B.iso3