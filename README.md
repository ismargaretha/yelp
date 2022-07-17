
# Yelp

The developed data pipeline translates the non-relational Yelp dataset distributed over JSON files, into a normalized Dimensional Model dataset stored in Snowflake. The resulting schema ensures data consistency and referential integrity across tables, and is meant to be the source of truth for analytical queries and BI tools. Additionally, the data was enriched with demographics and weather data coming from third-party data sources.

The entire process was done using dbt and Snowflake.



## Dataset

![image](https://upload.wikimedia.org/wikipedia/commons/a/ad/Yelp_Logo.svg)

The [Yelp Open Dataset](https://www.yelp.com/dataset) is a perfect candidate for this project, since:

- (1) it is a NoSQL data source;
- (2) it comprises of 6 files that count together more than 10 million rows;
- (3) this dataset provides lots of diverse information and allows for many analysis approaches, from traditional analytical queries (such as "Give me the average star rating for each city") to Graph Mining, Photo Classification, Natural Language Processing, and Sentiment Analysis;
- (4) Moreover, it was produced in a real production setting (as opposed to synthetic data generation).
To make the contribution unique, the Yelp dataset was enriched by demographics and weather data. This allows the end user to make queries such as "Does the number of ratings depend upon the city's population density?" or "Which restaurants are particularly popular during hot weather?".

tar -xvzf yelp_dataset.tar
