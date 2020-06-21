 # imports
import pyspark.sql.functions as F
import pyspark.sql.types as T
import datetime

# variables
s3_bucket = "s3://<s3-bucket>/"
raw_data_path = "{}rawdata/".format(s3_bucket)
processed_data_path = "{}processed/".format(s3_bucket)

# get data
us_city = spark.read.parquet("{}city/".format(processed_data_path))
us_state_code = spark.read.parquet("{}state_code/".format(processed_data_path))
us_airport = spark.read.parquet("{}airport/".format(processed_data_path))
us_weather = spark.read.parquet('{}weather/'.format(processed_data_path))

# build airport analytics dataframe
 analytics_airport = us_airport\
.select("name", "elevation_ft", "city_id")\
.join(us_city, 'city_id', 'left')\
.join(us_state_code, us_city.state_code == us_state_code.code, 'left')\
.join(us_weather, 'city_id', 'inner')\
.drop('state_code', 'code', 'city_id')

# write
analytics_airport.write.mode("overwrite").parquet("{}analytics_airport/".format(processed_data_path))