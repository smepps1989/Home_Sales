# Home_Sales
This module explores a home sales dataset using SparkSQL. Temporary views were created in order to consider various insights regarding the data. This module also compared query performance of SparkSQL when the dataset was partitioned using the parquet methos within SparkSQL and caching a table that initially only existed in temporary form. 

The following query was used to track the performance speeds of each method described:
```
spark.sql(
"""
  SELECT
    view, 
    ROUND(AVG(price / view), 2) AS avg_price_per_view
  FROM sales
  GROUP BY view
  HAVING AVG(price) > 350000
  ORDER BY avg_price_per_view DESC
"""
).show()
```
It was discovered that for this particular dataset, the query ran fastest when the table was cached. The query executed the slowest when the dataset was partitioned manually by the "date_built" field, even compared to the uncached and not partitioned temporary view. This is likely because the query is grouping by a field other than the partition key, meaning that all partition folders had to be searched in order to aggregate the request.


## Requirements
A Spark DataFrame is created from the dataset.

A temporary table of the original DataFrame is created.

A query is written that returns the average price, rounded to two decimal places, for a four-bedroom house that was sold in each year.

A query is written that returns the average price, rounded to two decimal places, of a home that has three bedrooms and three bathrooms for each year the home was built.

A query is written that returns the average price of a home with three bedrooms, three bathrooms, two floors, and is greater than or equal to 2,000 square feet for each year the home was built rounded to two decimal places.

A query is written that returns the average price of a home per "view" rating having an average home price greater than or equal to $350,000, rounded to two decimal places. (The output shows the run time for this query.)

A cache of the temporary "home_sales" table is created and validated.

The query from step 6 is run on the cached temporary table, and the run time is computed.

A partition of the home sales dataset by the "date_built" field is created, and the formatted parquet data is read.

A temporary table of the parquet data is created.

The query from step 6 is run on the parquet temporary table, and the run time is computed.

The "home_sales" temporary table is uncached and verified.
