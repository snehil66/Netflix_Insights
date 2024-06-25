select * from netflix_raw

create TABLE [dbo].[netflix_raw](
	[show_id] [varchar](10) primary key,
	[type] [varchar](10) NULL,
	[title] [nvarchar](200) NULL,
	[director] [varchar](250) NULL,
	[cast] [varchar](1000) NULL,
	[country] [varchar] (150) NULL,
	[date_added] [varchar](20) NULL,
	[release_year] [int] NULL,
	[rating] [varchar](10) NULL,
	[duration] [varchar](10) NULL,
	[listed_in] [varchar](100) NULL,
	[description] [varchar](500) NULL
)
GO

--Identifying Duplicates
select * from netflix_raw
where concat(title, type) in 
(
	select concat(title, type)
	from netflix_raw
	group by title, type
	having count(*) > 1
)
order by title;

---------------------------------------------------------------------------------------------------------

--Handling Duplicates
with cte as (
select *, row_number() over(partition by title, type order by show_id) rn
from netflix_raw
)
select show_id, type, title, cast(date_added as date) as date_added, 
release_year, rating, 
case when duration is null then rating else duration end as duration, description  
into netflix
from cte
--where rn=1

select show_id, value as director
from netflix_raw
cross apply string_split(director,',');

---------------------------------------------------------------------------------------------------------

--new table for listed_in, director, country and cast
select show_id, trim(value) as director
into netflix_director
from netflix_raw
cross apply string_split(director,',');

select show_id, trim(value) as country
into netflix_country
from netflix_raw
cross apply string_split(country,',');

select show_id, trim(value) as cast
into netflix_cast
from netflix_raw
cross apply string_split(cast,',');

select show_id, trim(value) as genre
into netflix_genre
from netflix_raw
cross apply string_split(listed_in,',');

---------------------------------------------------------------------------------------------------------

--populate missing value in country and director columns
insert into netflix_country
select show_id, m.country
from netflix_raw nr
inner join (
		select director, country
		from netflix_director nd
		inner join netflix_country nc
		on nd.show_id = nc.show_id
		group by director, country
		)m
on nr.director = m.director
where nr.country is null;



