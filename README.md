
# Yelp

The developed data pipeline translates the non-relational Yelp dataset distributed over JSON files, into a normalized dataset stored in Snowflake. The resulting schema ensures data consistency and referential integrity across tables, and is meant to be the source of truth for analytical queries and BI tools. For future development, this data can be enriched with other data from yelp, including tips, review, users, checkins. As well as additional data such as demographics or weather data coming from third-party data sources can be added to support the analysis.

Currently, this entire process was done using **Python**, **Snowflake**, and **Amazon S3 Bucket** for data storage. Moreover, other tools such as dbt/Fivetran can be added to simplify and automate the flow.


## Dataset

![image](https://upload.wikimedia.org/wikipedia/commons/a/ad/Yelp_Logo.svg)

The [Yelp Open Dataset](https://www.yelp.com/dataset) is a perfect candidate for this project, since:

- (1) it is a NoSQL data source;
- (2) it comprises of 6 files that count together more than 10 million rows;
- (3) this dataset provides lots of diverse information and allows for many analysis approaches, from traditional analytical queries (such as "Give me the average star rating for each city") to Graph Mining, Photo Classification, Natural Language Processing, and Sentiment Analysis;
- (4) Moreover, it was produced in a real production setting (as opposed to synthetic data generation).

As we are focusing on business data in this analysis, some examples of questions that can be answered are:
- What are the Top N restaurants in x neighborhood?
- How many users have reviewed this particular restaurant and what is the average stars given?
- What are the facilities offered by the restaurant? (e.g. support disability, TV, WiFi, accept bitcoin payment, etc.)
- etc.

Further development may include other data sources where the questions may expand, such as:
- Does the number of ratings depend upon the city's population density?
- Which restaurants are particularly popular during hot weather?
- etc.
## Yelp Open Dataset

The [Yelp Open Dataset](https://www.yelp.com/dataset) dataset is a subset of Yelp's businesses, reviews, and user data, available for academic use. The dataset (as of 13.08.2019) takes 9GB disk space (unzipped) and counts 6,685,900 reviews, 192,609 businesses over 10 metropolitan areas, over 1.2 million business attributes like hours, parking, availability, and ambience, 1,223,094 tips by 1,637,138 users, and aggregated check-ins over time. Each file is composed of a single object type, one JSON-object per-line. For more details on dataset structure, proceed to [Yelp Dataset JSON Documentation](https://www.yelp.com/dataset/documentation/main).
## Data model and dictionary

Our target data model is a normalized relational model, which is following the Kimball's Dimensional approach. The following image depicts the logical model of the database:

![image](https://github.com/ismargaretha/yelp/blob/f283bd312c9cf0f646704c9c5690c29ef741ae3e/data_model.png)

To understand Kimball’s approach to data modeling, we should begin by talking about the star schema. The star schema is a particular way of organizing data for analytical purposes. It consists of two types of tables:

A fact table, which acts as the primary table for the schema. 
- A fact table contains the primary measurements, metrics, or ‘facts’ of a business process.
- Many dimension tables associated with the fact table. Each dimension table contains ‘dimensions’ — that is, descriptive attributes of the fact table.
These dimensional tables are said to ‘surround’ the fact table, which is where the name ‘star schema’ comes from. The star schema is useful because it gives us a standardized, time-tested way to think about shaping your data for analytical purposes.


According to [Kimball Group](https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/kimball-techniques/dimensional-modeling-techniques/), the star schema is:
- Flexible — it allows your data to be easily sliced and diced any which way your business users want to.
- Extensible — you may evolve your star schema in response to business changes.
- Performant — Kimball’s dimensional modeling approach was developed when the majority of analytical systems were run on relational database management systems (RDBMSes). The star schema is particularly performant on RDBMSes, as most queries end up being executed using the ‘star join’, which is a Cartesian product of all the dimensional tables.

Note: fields such as goodformeal_* are just placeholders for multiple fields with the same prefix (goodformeal). This is done to visually reduce the length of the tables.

The model consists of 5 tables as a result of normalizing a table provided by Yelp. The schema is closer to a Star schema as there is only one fact table - t2_businesses - and 4 dimensional tables. Some tables keep their native keys, while for others monotonically increasing ids were generated. Rule of thumb: Use generated UID keys by Snowflake for entities.

### Pre-process data with Python prior to Snowflake stage ###

Prior to move data into Snowflake using a `copy into` command, data is being filtered to one state only **Florida** using Python in Jupyter Notebook. This was done for cost efficiency purpose before uploading data to the *S3 bucket as an external stage*, and prior to move the data to *Snowflake's internal stage*.
The full approach can be found in [Jupter Notebook: yelp_business_florida](https://github.com/ismargaretha/yelp/blob/28d93800f984cc959b6855133d38b92ff6e3ced0/yelp_business_florida.ipynb)

### Data Tranfer in Snowflake ###

The business.json data from Yelp dataset is staged, flattened, and normalized in Snowflake with the following table details:
Snowflake SQL codes to form below tables can be found here:  [Yelp Snowflake SQL](https://github.com/ismargaretha/yelp/blob/52addc37442ca4bfc74b8eb383d379a483a3ae3f/yelp_snowflake_sql.txt)

***t2_businesses***

The most referenced table in the model and acts as a Fact table. Contains the name of the business, the current star rating, the number of reviews, and whether the business is currently open, as well as the unique IDs of all dimension tables. The address (one-to-one relationship), hours (one-to-one relationship), businesses attributes (one-to-many relationship) and categories (one-to-one relationship).

***t2_business_attributes***

This was the most challenging part of the remodeling, since Yelp kept business attributes as nested dict. All fields in the source table were strings, so they had to be parsed into respective data types. Some values were dirty, for example, boolean fields can be `"True"`, `"False"`, `"None"` and `None`, while some string fields were of double unicode format `u"u'string'"`. Moreover, some fields were dicts formatted as strings. The resulting nested JSON structure of three levels had to be flattened. To flatten this, RegEx syntax with a help of `regexp_substr` function in Snowflake are used.


***t2_categories***

In the original table, business categories were stored as an array. The best solution was to outsource it into a separate table. However, since things should be kept simple here for this analysis, one category per business is extracted with case function.

```
case
    when categories ilike '%American%' then 'American'
    ...
end
```

As this was a part of the initial cleanup before moving to the t2_* table, this process was done upon the development of t1_businesses. Additionally, category_id is created using the substring of the category assigned as above.

`upper(left(categories, 4) || '000') as category_id`

***t2__hours***

Business hours were stored as a dict where each key is day of week and value is string of format `"hour:minute-hour:minute"`.
Primary key hours_id is derived from the saturday_hours field in t1_business with below codes:

```
case
    when saturday_hours  is null then '00000'
    else replace(saturday_hours, ':', '') 
end as hours_id
```

***t2__addresses***

A common practice is to separate business data from address data and connect them though a synthetic key. The resulting link is a one-to-one relationship. The primary key address_id itself was assigned using Snowflake UID function.

`right(uuid_string(), 12) as address_id`

This was done right after the staging process was done and variant data were transformed into structured data.


## Final Product

- 1 fact table: **t2_businesses** and 4 dimension tables: **t2_business_attributes**, **t2_addresses**, **t2_hours**, **t2_categories**
- Data is imported to Tableau to create a dashboard for data visualization purpose. 

![image](https://github.com/ismargaretha/yelp/blob/21adb1f23f6b5baaf1cdfc4df73c47ad30f47f84/Yelp%20Recommendations.png)
