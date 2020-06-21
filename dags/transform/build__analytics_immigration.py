 # imports
import pyspark.sql.functions as F
import pyspark.sql.types as T
import datetime

# variables
s3_bucket = "s3://<s3-bucket>/"
raw_data_path = "{}rawdata/".format(s3_bucket)
processed_data_path = "{}processed/".format(s3_bucket)

# get data
# codes
us_city = spark.read.parquet("{}city/".format(processed_data_path))
us_state_code = spark.read.parquet("{}state_code/".format(processed_data_path))
country_code = spark.read.parquet("{}country_code/".format(processed_data_path))

# immigration
us_immigrant = spark.read.parquet('{}immigrant/'.format(processed_data_path)).filter(F.col('monthYear')==F.lit(monthYear))
us_immigration = spark.read.parquet('{}immigration/'.format(processed_data_path)).filter(F.col('monthYear')==F.lit(monthYear))

# demographics
us_demographics = spark.read.parquet('{}demographics/'.format(processed_data_path))\
.select("median_age", "city_id", "total_population", "foreign_born")\
.join(city.select("state_code", "city_id"), "city_id")\
.drop('city_id')\
.groupBy("state_code")\
.agg(
    F.mean("median_age").alias('median_age'),
    F.sum("total_population").alias("total_population"),
    F.sum("foreign_born").alias("foreign_born")
)

# process anlaytics immigration
analytics_immigration = us_immigrant\
.select('cicid', 'from_country_code', 'age', 'occupation', 'gender', 'monthYear')\
.join(country_code, us_immigrant.from_country_code == country_code.code, 'left')\
.drop('from_country_code', 'code')\
.withColumnRenamed('country', 'from_country')\
.join(us_immigration.select('cicid','state_code'), 'cicid', 'left')\
.join(us_state_code, us_immigration.state_code == us_state_code.code, 'left')\
.drop('code')\
.join(us_demographics, 'state_code')\
.drop('state_code')

# write
analytics_immigration.write.partitionBy("monthYear").mode("append").parquet("{}analytics_immigration/".format(processed_data_path))

