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

# process raw airport data
us_airport = spark.read.csv("{}airports/airport-codes_csv.csv".format(raw_data_path), inferSchema=True, header=True)\
.filter(F.col("iso_country")==F.lit('US'))\
.withColumn("airport_lattitude", F.split("coordinates", ", ").getItem(0))\
.withColumn("airport_longitude", F.split("coordinates", ", ").getItem(1))\
.withColumn("state", F.split("iso_region", "-").getItem(1))\
.withColumnRenamed("ident", "icao_code")\
.join(us_city, (F.col("municipality")==city.city) & (F.col("state")==city.state_code), 'left')\
.drop("coordinates", "gps_code", "local_code", "continent", "iso_region", "iso_country", "municipality", "state", "city", "state_code")

# write 
us_airport.write\
.coalesce(4)\
.mode("overwrite")\
.parquet("{}airport/".format(processed_data_path))

