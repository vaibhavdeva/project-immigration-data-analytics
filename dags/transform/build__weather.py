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

# global temperature data is recorded since 1700 limiting the scope by date threshold
date_threshold = F.to_date(F.lit("2013-08-01")).cast(T.TimestampType())

# precess weather data for united states
us_weather = spark.read.csv("{}weather/GlobalLandTemperaturesByCity.csv".format(raw_data_path), inferSchema=True, header=True)\
.where( (F.col('dt')>date_threshold) & (F.col("AverageTemperature").isNotNull()) & (F.col("Country")==F.lit("United States")) )\
.withColumn("lattitude", 
            F.when( F.array_contains( F.split("Latitude", ""), "N"), 
                  F.expr("substring(Latitude, 1, length(Latitude)-1)")).
            otherwise( -1 * F.expr("substring(Latitude, 1, length(Latitude)-1)") ))\
.withColumn("longitude",
           F.when( F.array_contains( F.split("Longitude", ""), "E"), 
                  F.expr("substring(Longitude, 1, length(Longitude)-1)")).
            otherwise( -1 * F.expr("substring(Longitude, 1, length(Longitude)-1)")))\
.withColumnRenamed("AverageTemperature", "avg_temp")\
.withColumnRenamed("AverageTemperatureUncertainty", "std_temp")\
.withColumnRenamed("City", "city")\
.withColumnRenamed("Country", "country")\
.join(us_city, "city", "left")\
.drop("dt", "Country", "Latitude", "city", "state_code")


# write
us_weather.write\
.mode('overwrite')\
.parquet('{}weather/'.format(processed_data_path))
