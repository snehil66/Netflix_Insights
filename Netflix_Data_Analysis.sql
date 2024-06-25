--Netflix Data Analysis

/*1.  for each director count the no of movies and tv shows created by them in separate columns 
for directors who have created tv shows and movies both */
select nd.director,
count(distinct case when n.type = 'Movie' then n.show_id end) as no_of_movies,
count(distinct case when n.type = 'Tv Show' then n.show_id end) as no_of_tvshows
from netflix n
inner join netflix_director nd 
on n.show_id = nd.show_id
group by nd.director
having count(distinct n.type)>1

---------------------------------------------------------------------------------------------------------

--2.which country has highest no. of comedy movies
select  nc.country, count(ng.genre) as Comedy_movies
from netflix_genre ng
join netflix_country nc on ng.show_id = nc.show_id
inner join netflix n on ng.show_id = n.show_id	
where ng.genre = 'Comedies' and n.type = 'Movie'
group by nc.country
order by Comedy_movies desc

---------------------------------------------------------------------------------------------------------

/*3. For each year (as per date added in netflix table), which director has maximum no. of 
movies released.*/
with cte as (
	select nd.director, year(n.date_added) as release_year, count(n.show_id) as no_of_movies
	from netflix n
	inner join netflix_director nd 
	on n.show_id = nd.show_id
	where type = 'movie'
	group by nd.director, year(n.date_added)
),
cte2 as(
	select *, ROW_NUMBER() over(partition by release_year order by no_of_movies desc, director) rn
	from cte
)
select * from cte2 where rn=1

---------------------------------------------------------------------------------------------------------

--4. What is the average duration of movies in each genre
select ng.genre, avg(cast(REPLACE(duration, ' min', '') as int)) as avg_duration
from netflix n
inner join netflix_genre ng on n.show_id = ng.show_id
where n.type = 'Movie'
group by ng.genre

---------------------------------------------------------------------------------------------------------

/*5.Find the list of directors who have created horror and comedy both movies display director names 
along with the number of comedy and horror movies both*/
select nd.director as director, 
count(distinct case when ng.genre = 'Comedies' then n.show_id end) as no_of_comedy_movies,
count(distinct case when ng.genre = 'Horror Movies' then n.show_id end) as no_of_horror_movies
from netflix_director nd
inner join netflix_genre ng on nd.show_id = ng.show_id
inner join netflix n on n.show_id = ng.show_id
where type = 'Movie'and genre in ('Comedies', 'Horror Movies')
group by nd.director
having count(distinct ng.genre) = 2;
