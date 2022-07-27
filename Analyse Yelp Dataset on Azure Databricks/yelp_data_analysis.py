# Databricks notebook source
# MAGIC %md
# MAGIC #Set Connection to ADLS

# COMMAND ----------

#Connect to ADLS
spark.conf.set('fs.azure.account.key.yelpprojectstorage.dfs.core.windows.net', 'SxK5rbjNin+OUDsfA0YH7fVkDU2iIpKcZZ5FxIviKKasbb9bBKOUTR2QyXOAe67D67F5wjJiI6m7+ASteMd5PA==')

# COMMAND ----------

#List datasets
dbutils.fs.ls("abfss://yelp-target@yelpprojectstorage.dfs.core.windows.net/")

# COMMAND ----------

#Convert to parquet in new folder
new_path = "abfss://yelp-target@yelpprojectstorage.dfs.core.windows.net/parquet_files/"
suffix = '.parquet'
for path in dbutils.fs.ls("abfss://yelp-target@yelpprojectstorage.dfs.core.windows.net/"):
    file = path.path[82:]
    file = file[:-5]
    write_path = new_path + file + suffix
    df = spark.read.json(path.path)
    df.write.mode('overwrite').parquet(write_path)

# COMMAND ----------

#Convert to Delta Files
new_path = "abfss://yelp-target@yelpprojectstorage.dfs.core.windows.net/yelp_delta_files/"
suffix = '.delta'
for path in dbutils.fs.ls("abfss://yelp-target@yelpprojectstorage.dfs.core.windows.net/"):
    if path.path[-4:] == 'json':
        file = path.path[82:]
        file = file[:-5]
        write_path = new_path + file + suffix
        df = spark.read.json(path.path)
        df.write.format("delta").mode("overwrite").save(write_path)
    else:
        continue

# COMMAND ----------

# MAGIC %md
# MAGIC #Load in delta dataframes for analysis
# MAGIC ###Practice Repartitions & Coalesce

# COMMAND ----------

#Confirm same row count across file types
business_df = spark.read.format("delta").load("abfss://yelp-target@yelpprojectstorage.dfs.core.windows.net/yelp_delta_files/business.delta")
print(business_df.count())
business_df = spark.read.format("parquet").load("abfss://yelp-target@yelpprojectstorage.dfs.core.windows.net/parquet_files/business.parquet")
print(business_df.count())
business_df = spark.read.format("json").load("abfss://yelp-target@yelpprojectstorage.dfs.core.windows.net/yelp_academic_dataset_business.json")
print(business_df.count())


# COMMAND ----------

#Load in dataframes 
business_df = spark.read.format("delta").load("abfss://yelp-target@yelpprojectstorage.dfs.core.windows.net/yelp_delta_files/business.delta")
checkin_df = spark.read.format("delta").load("abfss://yelp-target@yelpprojectstorage.dfs.core.windows.net/yelp_delta_files/checkin.delta")
review_df = spark.read.format("delta").load("abfss://yelp-target@yelpprojectstorage.dfs.core.windows.net/yelp_delta_files/review.delta")
tip_df = spark.read.format("delta").load("abfss://yelp-target@yelpprojectstorage.dfs.core.windows.net/yelp_delta_files/tip.delta")
user_df = spark.read.format("delta").load("abfss://yelp-target@yelpprojectstorage.dfs.core.windows.net/yelp_delta_files/user.delta")

print('business df: ', business_df.count())
print('checkin df: ', checkin_df.count())
print('review df: ', review_df.count())
print('tip df: ', tip_df.count())
print('user df: ', user_df.count())

# COMMAND ----------

display(review_df)

# COMMAND ----------

#Extract month, year 
from pyspark.sql import functions as f
review_df = review_df.withColumn('Year', f.year(review_df.date))
review_df = review_df.withColumn('Month', f.month(f.col('date')))

# COMMAND ----------

display(review_df)

# COMMAND ----------

#Export review_df partitioned by year then month
review_df.write.mode('overwrite').partitionBy('Year', 'Month').parquet("abfss://yelp-target@yelpprojectstorage.dfs.core.windows.net/reviews_partitioned_by_year_month/")

# COMMAND ----------

#Investigate number of partitions (Storage account says 25)
user_df.rdd.getNumPartitions()
#Matches

# COMMAND ----------

#Current Partitions uneven, try Coalesce and Repartition methods on user
#reduce with coalesce
user_reduced_c = user_df.coalesce(10)
print('Coalesce:', user_reduced_c.rdd.getNumPartitions())

user_reduced_r = user_df.repartition(10)
print('Repartitioned:', user_reduced_c.rdd.getNumPartitions())

# COMMAND ----------

#Inspect raw files in container
user_reduced_c.write.parquet("abfss://yelp-target@yelpprojectstorage.dfs.core.windows.net/coalesce/")
user_reduced_r.write.parquet("abfss://yelp-target@yelpprojectstorage.dfs.core.windows.net/repartition/")

# COMMAND ----------

# MAGIC %md
# MAGIC #Answering Business Questions

# COMMAND ----------

#Allow sql to work w spark dataframe
user_df.createOrReplaceTempView("user")

# COMMAND ----------

# MAGIC %sql
# MAGIC --Top 10 users with most fans
# MAGIC SELECT user_id, fans 
# MAGIC FROM user
# MAGIC ORDER BY fans DESC
# MAGIC limit 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC --Order by most reviews
# MAGIC SELECT name, user_id, review_count
# MAGIC FROM user
# MAGIC ORDER BY review_count DESC
# MAGIC LIMIT 3;

# COMMAND ----------

#Top 10 categories #not counting same place as several reviews; just 1
from pyspark.sql.window import Window
cat_count = business_df.groupBy('categories').agg(f.count(business_df.review_count).alias('reviews'))
cat_count = cat_count.withColumn('rank',f.row_number().over(Window.orderBy(f.col('reviews').desc())))
cat_count = cat_count.filter("rank <= 10")
display(cat_count)

# COMMAND ----------

business_df.createOrReplaceTempView('business')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT categories, COUNT(review_count)
# MAGIC FROM business
# MAGIC GROUP BY 1
# MAGIC ORDER BY 2 DESC
# MAGIC LIMIT 10;

# COMMAND ----------

#Categories with more than 1000 reviews
cat_count = business_df.groupBy('categories').agg(f.count('review_count').alias('reviews'))
cat_count = cat_count.withColumn('rank', f.row_number().over(Window.orderBy(f.col('reviews').desc())))
cat_count.show()

# COMMAND ----------

#Number of Restaurants per State
states = business_df.groupBy('State').agg(f.count('name').alias('Restaurants')).orderBy('Restaurants', ascending=False)
display(states)

# COMMAND ----------

#Top 3 Restaurants by State, review count
business_df.createOrReplaceTempView('restaurants')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM (SELECT state, name, review_count,
# MAGIC       ROW_NUMBER() OVER (PARTITION BY state ORDER BY review_count DESC) as rank
# MAGIC       FROM restaurants)
# MAGIC WHERE rank <= 3;

# COMMAND ----------

#List top restaurants in Arizona by reviews
arizona = business_df.filter("state == 'AZ'")
arizona = arizona.groupBy('name').agg(f.count('review_count').alias('reviews'))
arizona = arizona.withColumn('rank', f.row_number().over(Window.orderBy(f.desc('reviews'))))
arizona = arizona.select('name', 'rank', 'reviews')
display(arizona)

# COMMAND ----------

#Restaurant count by state and city
state_city = business_df.groupBy("state", "city").count().orderBy(f.desc('count'))

display(state_city.filter("state == 'AZ'"))

# COMMAND ----------

#City with most
top_city = state_city.filter("city == 'Tucson' and state == 'AZ'")
print(type(top_city))
display(top_city)

# COMMAND ----------

#Broadcast Join
#Smaller df so it's available in memory no shuffling during join (bc of partitions)
from pyspark.sql.functions import broadcast
#df top city + df business
new_df = business_df.join(broadcast(top_city), on='city', how='inner')
display(new_df)

# COMMAND ----------

#Most Reviewed Italian Restaurant
italian_df = business_df.filter("LOWER(categories) LIKE '%italian%'")
#display(italian_df.select('name', 'review_count', 'categories').orderBy(f.desc('review_count')))
display(italian_df.select('name', 'review_count', 'categories').orderBy('review_count', ascending=False).take(10))
