# imports
import pyspark.sql.functions as F
import pyspark.sql.types as T
import datetime

# variables
s3_bucket = "s3://<s3-bucket>/"
raw_data_path = "{}rawdata/".format(s3_bucket)
processed_data_path = "{}processed/".format(s3_bucket)

# get us cities processed data
us_city = spark.read.parquet("{}city/".format(processed_data_path))

# process demographics
us_demographics = spark.read.csv("{}demographics/us-cities-demographics.csv".format(raw_data_path), inferSchema=True, header=True, sep=';')\
.select(
    F.col("Male Population").cast(T.LongType()).alias("male_population"),
    F.col("Female Population").cast(T.LongType()).alias("female_population"),
    F.col("Total Population").cast(T.LongType()).alias("total_population"),
    F.col("Number of Veterans").cast(T.LongType()).alias("num_veterans"),
    F.col("Foreign-born").cast(T.LongType()).alias("foreign_born"),
    F.col("Average Household Size").alias("avg_household_size"),
    F.col("State Code").alias("state_code"),
    F.col("Race").alias("race"),
    F.col("Median Age").alias("median_age"),
    F.col("City").alias('city')
)\
.join(us_city, ['city', 'state_code'])\
.drop('city', 'state_code')

# write
us_demographics.write\
.coalesce(4)\
.mode('overwrite')\
.parquet('{}demographics/'.format(processed_data_path))
