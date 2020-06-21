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


# function to check data availability
def dataCheck(path, isFilter=True):
	try:

		if(isFilter):
			df = spark.read.parquet(path).filter(F.col("monthYear")==F.lit(month_year))
		else:
			df = spark.read.parquet(path)

		if( (len(df.columns)>0) & df.count()>0 ):
			mylogger.warn("All good with {}".format(path.split("/")[-2]))
		else:
			mylogger.warn("Data path is ok, with {}, but there is no data".format(path.split("/")[-2]))
	except:
		mylogger.warn("Check failed for {}".format(path.split("/")[-2]))

# builds checks
dataCheck("{}city/".format(processed_data_path), isFilter=False)
dataCheck("{}state_code/".format(processed_data_path), isFilter=False)
dataCheck("{}country_code/".format(processed_data_path), isFilter=False)
dataCheck("{}airport/".format(processed_data_path), isFilter=False)
dataCheck("{}demographics/".format(processed_data_path), isFilter=False)
dataCheck("{}weather/".format(processed_data_path), isFilter=False)
dataCheck("{}immigrant/".format(processed_data_path), isFilter=True)
dataCheck("{}immigration/".format(processed_data_path), isFilter=True)

# analytics checks
dataCheck("{}analytics_airport/".format(processed_data_path), isFilter=False)
dataCheck("{}analytics_immigration/".format(processed_data_path), isFilter=True)