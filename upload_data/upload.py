
# import
import boto3
import configparser
import os

# set environment variables for acces key and secret key
config = configparser.ConfigParser()
config.read('aws.cfg')
os.environ["AWS_ACCESS_KEY_ID"] = config.get("AWS", "AWS_ACCESS_KEY_ID")
os.environ["AWS_SECRET_ACCESS_KEY"] = config.get("AWS", "AWS_SECRET_ACCESS_KEY")

# get s3 client
s3 = boto3.client('s3')
bucket_name = '<s3-bucket>'

# upload files to s3

# upload immigration data
root = "../../data/18-83510-I94-Data-2016/"
files = [root+f for f in os.listdir(root)]
for f in files:
    s3.upload_file(f, bucket_name, "/rawdata/i94_immigration/18-83510-I94-Data-2016/" + f.split("/")[-1])
    
# upload demographics data
s3.upload_file("data/us-cities-demographics.csv", bucket_name,  "/rawdata/demographics/us-cities-demographics.csv")

# upload weather data
s3.upload_file("../../data2/GlobalLandTemperaturesByCity.csv", bucket_name,  "/rawdata/weather/GlobalLandTemperaturesByCity.csv")

# upload airport data
s3.upload_file("data/airport_code.csv", bucket_name, "/rawdata/airports/airport_code.csv")

# uplooad codes
s3.upload_file("data/state_code.csv", bucket_name, "/rawdata/codes/state_code.csv")

s3.upload_file("data/country_code.csv", bucket_name, "/rawdata/codes/country_code.csv")
