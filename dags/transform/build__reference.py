
# This script combines all the cities in airports and demographics dataset
# write the results back to s3 in parquet format so that it can be used for 
# further analysis in spark


# imports
import pyspark.sql.functions as F
import pyspark.sql.types as T
import datetime

# variables
s3_bucket = "s3://<s3-bucket>/"
raw_data_path = "{}rawdata/".format(s3_bucket)
processed_data_path = "{}processed/".format(s3_bucket)

# get cities information from demographics data
us_demographics = spark.read.csv("{}demographics/us-cities-demographics.csv".format(raw_data_path), inferSchema=True, header=True, sep=";")\
.select(
	F.col("State Code").alias("state_code"), 
	F.col("City").alias("city")
)

# get cities information from airport data
us_airport = spark.read.csv("{}airports/airport-codes_csv.csv".format(raw_data_path), inferSchema=True, header=True)\
.filter(F.col("iso_country")==F.lit("US"))\
.withColumn("state_code", F.split("iso_region", "-").getItem(1))\
.select("state_code", F.col("municipality").alias("city"))

# combine all the cities to get all possible cities along with their state codes
us_cities = us_airport\
.union(us_demographics)\
.dropDuplicates()\
.withColumn("city_id", F.monotonically_increasing_id())

# write back to s3
us_cities.write\
.coalesc(2)\
.mode("overwrite")\
.parquet("{}city/".format(processed_data_path))

# get all the states code and write them back to s3 in parquet format
spark.read.format('csv')\
.load('{}codes/state_code.csv'.format(raw_data_path, header=True, inferSchema=True))\
.write\
.coalesc(1)\
.mode("overwrite")\
.parquet("{}state_code/".format(processed_data_path))

# get all teh country codes and write them back to s3 in parquet format country_code.csv
spark.read.format('csv')\
.load('{}codes/country_code.csv'.format(raw_data_path, header=True, inferSchema=True))\
.write\
.coalesc(1)\
.mode("overwrite")\
.parquet("{}country_code/".format(processed_data_path))


