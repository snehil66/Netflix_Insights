# Netflix_Insights

### Netflix Data Analysis Report

#### Introduction
The aim of this analysis is to provide insights into Netflix's content library using data extracted from Kaggle and loaded into MSSQL. The dataset includes information on movies and TV shows available on Netflix, including details such as title, director, cast, country, date added, release year, rating, duration, and genres. The analysis focuses on various aspects of the data, such as director contributions, genre distribution, country-specific content, and average duration of movies.

#### Data Extraction and Loading
The dataset was extracted from Kaggle and loaded into MSSQL using Python. The following steps were undertaken:
1. **Data Extraction**: The data was extracted using the Kaggle API and loaded into a Pandas DataFrame.
2. **Data Loading**: The DataFrame was then loaded into MSSQL using SQLAlchemy.

```python
import pandas as pd 
import sqlalchemy as sal

df = pd.read_csv('netflix_titles.csv')
engine = sal.create_engine('mssql://ANKIT\SQLEXPRESS/master?driver=ODBC+DRIVER+17+FOR+SQL+SERVER')
conn = engine.connect()
df.to_sql('netflix_raw', con=conn, index=False, if_exists='append')
conn.close()
```

#### Data Cleaning
The data cleaning process involved the following steps:
1. **Schema Definition**: A new table `netflix_raw` was created with defined data types.
2. **Duplicate Identification**: Duplicates were identified and handled using Common Table Expressions (CTEs).
3. **Column Normalization**: Columns like `director`, `country`, `cast`, and `listed_in` were split into separate rows for better analysis.

SQL commands used for cleaning and normalizing data:
```sql
-- Schema Definition
create TABLE [dbo].[netflix_raw](
	  primary key,
	  NULL,
	  NULL,
	  NULL,
	  NULL,
	  NULL,
	  NULL,
	[release_year] [int] NULL,
	  NULL,
	  NULL,
	  NULL,
	  NULL
);
```

#### Detailed Analysis
The analysis performed included several key metrics and insights:

1. **Director Contributions**: 
   - Count of movies and TV shows created by directors along with their names who have worked on both types of content .
   ```sql
   select nd.director,
   count(distinct case when n.type = 'Movie' then n.show_id end) as no_of_movies,
   count(distinct case when n.type = 'Tv Show' then n.show_id end) as no_of_tvshows
   from netflix n
   inner join netflix_director nd 
   on n.show_id = nd.show_id
   group by nd.director
   having count(distinct n.type) > 1;
   ```
   - **Insight**: Directors who have diversified their portfolio by creating both movies and TV shows were identified, aiding in understanding their versatility.

2. **Country with Highest Number of Comedy Movies**:
   - Identification of countries producing the highest number of comedy movies.
   ```sql
   select  nc.country, count(ng.genre) as Comedy_movies
   from netflix_genre ng
   join netflix_country nc on ng.show_id = nc.show_id
   inner join netflix n on ng.show_id = n.show_id	
   where ng.genre = 'Comedies' and n.type = 'Movie'
   group by nc.country
   order by Comedy_movies desc;
   ```
   - **Metric**: The country with the highest number of comedy movies is identified for better market analysis.

3. **Top Directors by Year**:
   - Directors with the maximum number of movies released each year.
   ```sql
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
   select * from cte2 where rn = 1;
   ```
   - **Metric**: Year-wise top directors provide insights into trends and popularity over time.

4. **Average Duration of Movies by Genre**:
   - Calculation of the average duration of movies across different genres.
   ```sql
   select ng.genre, avg(cast(REPLACE(duration, ' min', '') as int)) as avg_duration
   from netflix n
   inner join netflix_genre ng on n.show_id = ng.show_id
   where n.type = 'Movie'
   group by ng.genre;
   ```
   - **Metric**: Understanding the average duration helps in content planning and user engagement strategies.

5. **Directors Creating Both Comedy and Horror Movies**:
   - Identifying directors who have worked on both comedy and horror genres.
   ```sql
   select nd.director as director, 
   count(distinct case when ng.genre = 'Comedies' then n.show_id end) as no_of_comedy_movies,
   count(distinct case when ng.genre = 'Horror Movies' then n.show_id end) as no_of_horror_movies
   from netflix_director nd
   inner join netflix_genre ng on nd.show_id = ng.show_id
   inner join netflix n on n.show_id = ng.show_id
   where type = 'Movie' and genre in ('Comedies', 'Horror Movies')
   group by nd.director
   having count(distinct ng.genre) = 2;
   ```
   - **Metric**: Directors who have created both **Comedy and Horror genres*** indicate versatility and capability to appeal to diverse audience preferences.

#### Conclusion
The analysis provided detailed insights into the Netflix content library, identifying key trends and metrics related to directors, genres, and country-specific content. These insights are crucial for strategic decision-making in content acquisition, production, and market expansion. The comprehensive ELT process ensures data accuracy and reliability, forming a strong foundation for advanced analytics and business intelligence.
 ---
This report demonstrates the power of data analytics in transforming raw data into actionable insights, enabling Netflix to maintain its competitive edge in the streaming industry.
