# imports
import pyspark.sql.functions as F
import pyspark.sql.types as T
import datetime

# variables
s3_bucket = "s3://<s3-bucket>/"
raw_data_path = "{}rawdata/".format(s3_bucket)
processed_data_path = "{}processed/".format(s3_bucket)

# user definded functions
@F.udf(T.TimestampType())
def convSasDate(daysCount):
    import datetime
    sas_ref = datetime.datetime(1960,1,1)
    try:
        return sas_ref + datetime.timedelta(days=int(daysCount))
    except:
        return daysCount


# splitting immigration data into 2 datasets 
# first one is for immigrants
# second one is for immigration data

# process immigration data for immigrant
us_immigrant = spark.read.format('com.github.saurfang.sas.spark')\
.load("{}i94_immigration/18-83510-I94-Data-2016/i94_{}_sub.sas7bdat".format(raw_data_path, month_year))\
.withColumn("gender", F.when(F.col("gender")==F.lit("X"), F.lit("O")).otherwise(F.col("gender")))\
.select(
    F.col("cicid").cast(T.IntegerType()).alias("cicid"),
    F.col("i94res").cast(T.IntegerType()).alias("from_country_code"),
    F.col("i94bir").cast(T.IntegerType()).alias("age"),
    F.col("i94visa").cast(T.IntegerType()).alias("visa_code"),
    F.col("visapost").alias("visa_post"),
    F.col("occup").alias("occupation"),
    F.col("visatype").alias("visa_type"),
    F.col("biryear").cast(T.IntegerType()).alias("birth_year"),
    F.col("gender")
)\
.withColumn("monthYear", F.lit(month_year))

# write
us_immigrant.write.partitionBy("monthYear").mode("append").parquet('{}immigrant/'.format(processed_data_path))


# process immigration data for immigration stats
us_immigration = spark.read.format('com.github.saurfang.sas.spark')\
.load("{}i94_immigration/18-83510-I94-Data-2016/i94_{}_sub.sas7bdat".format(raw_data_path, month_year))\
.select(
    F.col("cicid").cast(T.IntegerType()).alias("cicid"),
    F.col("admnum").cast(T.LongType()).alias("admnum"),
    F.col("i94port").alias("iata_code"),
    F.col("i94addr").alias("state_code"),
    "arrdate","depdate", "dtaddto", "airline", "fltno", "entdepa", "entdepd", "entdepu", "matflag"
)\
.withColumn("arrival_date", convSasDate("arrdate"))\
.withColumn("departure_date", convSasDate("depdate"))\
.withColumn("deadline_departure", F.unix_timestamp("dtaddto", 'mmddyyyy').cast(T.TimestampType()))\
.withColumn("monthYear", F.lit(month_year))\
.drop("arrdate", "depdate", "dtaddto")

# write
us_immigration.write.partitionBy("monthYear").mode('append').parquet('{}immigration/'.format(processed_data_path))




