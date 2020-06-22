 # imports
import pyspark.sql.functions as F
import pyspark.sql.types as T

#logger
spark.sparkContext.setLogLevel('WARN')
Logger= spark._jvm.org.apache.log4j.Logger
mylogger = Logger.getLogger("DAG")


# variables
s3_bucket = "s3://<s3-bucket>/"
raw_data_path = "{}rawdata/".format(s3_bucket)
processed_data_path = "{}processed/".format(s3_bucket)

expected_output = {
	"avg_temperature_california":22.529
	"min_age_of_immigrant_from_afghanistan":2
	"max_age_of_immigrant_from_afghanistan":86
}

# Test 1
airport_anaytics_data = spark.read.parquet("{}analytics_airport/".format(processed_data_path))
avg_temp = airport_anaytics_data\
.groupBy("state")\
.agg(
	F.mean(F.col("avg_temp")).alias("avg_temp")
)\
.filter(F.col('state')==F.lit('CALIFORNIA'))\
.collect()
avg_temp = [round(i[1], 2) for i in avg_temp]
if(avg_temp == expected_output['avg_temperature_california']):
	mylogger.warn("First test PASSED")
else:
	mylogger.warn("First test FAILED")

# Test 2
immigration_analytics_data = spark.read.parquet("{}analytics_immigration/".format(processed_data_path))
ages = immigration_analytics_data\
.filter((F.col('i94_dt')==F.lit('apr16')) & (F.col('from_country')==F.lit('AFGHANISTAN')))\
.groupBy('from_country')\
.agg(
	F.max(F.col('age')).alias('maximum_age_of_immigrant'), 
	F.min(F.col('age')).alias('minimum_age_of_immigrant')
)\
.collect()

minAge = [i[2] for i in ages]
maxAge = [i[1] for i in ages]

# Test 2
if(minAge == expected_output['min_age_of_immigrant_from_afghanistan']):
	mylogger.warn("Second test PASSED")
else:
	mylogger.warn("Second test FAILED")

# Test 3
if(maxAge == expected_output['max_age_of_immigrant_from_afghanistan']):
	mylogger.warn("Third test PASSED")
else:
	mylogger.warn("Third test FAILED")