# Project description
- Lot of people migrate to and from US every year, which creates a huge task of tracking each and every immigrant in the country difficult. Luckily a huge ammount of data gets generated for every immigrant, the data about their travel, destination, visa type etc. When combined this data with weather and demographic data would certainly give some insights about immigrants such as, the peak period when there are lot of application for immigration, which sate in US is prefered etc.

- With data data available I have built a robust and scalable pipeline in Airflow using Amazon Web Services such as using S3, EMR and postgres. this pipeline moves raw data to S3 and transforms it into a data model which lets the regulators trach individual immigrants easily.


# Data sources
- In this project I am using the data provided by Udacity for capstone project for Data Engineering nano degree program. Below is the list of datasets being used,
    1. I94 Immigration Data: This data comes from the US National Tourism and Trade Office [Source](https://travel.trade.gov/research/reports/i94/historical/2016.html). This data records immigration records partitioned by month of every year.
    2. World temperature Data: This dataset comes from Kaggle [Source](https://www.kaggle.com/berkeleyearth/climate-change-earth-surface-temperature-data). Includes temperature recordings of cities around the world for a period of time
    3. US City Demographic Data: This dataset comes from OpenSoft [Source](https://public.opendatasoft.com/explore/dataset/us-cities-demographics/export/). Includes population formation of US states, like race and gender.
    4. Aiport Code table: [Source](https://datahub.io/core/airport-codes#data). Includes a collection of airport codes and their respective cities, countries around the world.
- There is one more data which description of i94 immigration dataset. It contains country and state codes which I will be using in the analysis.
- The data exploration, observations about the data are all done in pandas on a sampe data. This exploration is done in `Immigration data explore.ipynb` file.


# Data model and design
## Data Model
- In this project I will be using a Star model
    - Easey to implement and understand
    - flexible

## Table designs
1. In this i have created a total of 9 tables out of which 
    - 3 are reference tables for City, Country and State
    - 4 are dimmension tables creating using reference and raw data
    - 2 are facts tables for analytics

## Data dictionary
1. <b>Reference Tables</b>
    1. <b>city (using airport and demographic data)</b>
        table | column_name | type | contains_null? | example_value 
        ---|---|---|---|---
        city | state_code | str | False | MI
        city | city | str | True | Grant
        city | city_id | int | False | 0

    2. <b>country_code (from i94 immigration data description file)</b>
        table | column_name | type | contains_null? | example_value 
        ---|---|---|---|---
        country_code | code | int | False | 236
        country_code | country | str | False | AFGHANISTAN

    3. <b>state_code (from i94 immigration data description file)</b>
        table | column_name | type | contains_null? | example_value 
        ---|---|---|---|---
        state_code | code | str | False | AL
        state_code | state | str | False | ALABAMA

2. <b>Dimmension table</b>
    1. <b>airports (From data sources #4)</b>
        table | column_name | type | contains_null? | example_value | comment
        ---|---|---|---|---|---
        airport | icao_code | str | False | 00A | airport ident code
        airport | type | str | False | heliport | airport type
        airport | name | str | False | Total Rf Heliport | airport name
        airport | elevation_ft | float | True | 11.0 | elevation of airport from sea level
        airport | iata_code | str | True | 00A | international airport code
        airport | airport_latitude | str | False | -74.93360137939453 | 
        airport | airport_longitude | str | False | 40.07080078125 | 
        airport | city_id | float | True | 893353197568.0 | From city table in reference tanle

    2. <b>demographics (From data sources #3)</b>
        table | column_name | type | contains_null? | example_value | comment
        ---|---|---|---|---|---
        demographic | male_population | float | True | 410615.0 | total male population
        demographic | female_population | float | True | 437808.0 | total female population
        demographic | total_population | int | False | 848423 | total population
        demographic | num_veterans | float | True | 42186.0 | total number of vaterans
        demographic | foreign_born | float | True | 72456.0 | total number of people who are foreign born
        demographic | avg_household_size | float | True | 2.53 | on an average the size of a family
        demographic | race | str | False | White | 
        demographic | median_age | float | False | 34.1 | median age of the group of people living the that city
        demographic | city_id | int | False | 3 |

    3. <b>weather</b>
        table | column_name | type | contains_null? | example_value | comment
        ---|---|---|---|---|---
        weather | avg_temp | float | False | 25.791 | average temperature of the city
        weather | std_temp | float | False | 1.18 | average uncertainty in the temperature of the city
        weather | latitude | float | False | 40.07 |
        weather | longitude | float | False | -100.53 |
        weather | city_id | int | False | 1640677507127 |

    4. <b>immigrants ( from i94 immigration data)</b>
        table | column_name | type | contains_null? | example_value | comment
        ---|---|---|---|---|---
        immigrant | cicid | int | False | 6 | immigrants reference id
        immigrant | from_country_code | int | False | 692 | the country where the immigrants came from
        immigrant | age | int | False | 37 | 
        immigrant | visa_code | int | False | 2 | reason of travel
        immigrant | visa_post | str | True | GUZ | the department of state where the visa was issued
        immigrant | occupation | str | True | STU | occupation of immigrant
        immigrant | visa_type | str | False | B2 | type of visa issue
        immigrant | birth_year | int | False | 1979 |  year of birth
        immigrant | gender | str | True | F | 
        immigrant | monthYear | str | False | apr16 | month and year of the data arrival

    5. <b>immigration (from i94 immigration data)</b>
        table | column_name | type | contains_null? | example_value | comment
        ---|---|---|---|---|---
        immigration | cicid | int | False | 6 |
        immigration | admnum | int | False | 1897628485 | admission number
        immigration | iata_code | str | False | XXX | international airport code
        immigration | state_code | str | True | HI |
        immigration | airline | str | True | JL | name of the airline used by the immigrant for travel
        immigration | fltno | str | True | 00782 | flight number
        immigration | entdepa | str | False | T | arrival flag
        immigration | entdepd | str | True | O | departure flag
        immigration | entdepu | str | True | U | updated flag
        immigration | matflag | str | True | M | match flag
        immigration | arrival_date | timestamp | False | 2016-04-29 00:00:00 | date of arrival
        immigration | departure_date |timestamp | True | 2016-02-29 00:00:00 | date of departure
        immigration | deadline_departure | timestamp | True | 2016-01-28 00:10:00 | deadline date of departure for corrosponding immigrant
        immigration | monthYear | str | False | apr16 | month and year of the data

3. <b>Analytics table (facts)</b>
    1. <b>airport analytics (using transformed city, airport, weather, state_code and country code data)</b>
        table | column_name | type | contains_null? | example_value | comment
        ---|---|---|---|---|---
        analytics_airport | name | str | False | Arnold Ranch Airport | name of airport
        analytics_airport | elevation_ft | int | False | 382 | elevation of airport location from sea level in ft.
        analytics_airport | city | str | False | Fresno | name of the city
        analytics_airport | state | str | False | CALIFORNIA | name of the state
        analytics_airport | avg_temp | float | False | 24.186 | average temperature at the location
        analytics_airport | std_temp | float | False | 0.797 | average uncertainty in the temperature at the location
        analytics_airport | longitude | float | False | -119.34 |
        analytics_airport | latitude | float | False | 40.07 |

    2. <b>immigration analytics (using transformed city, state_code, country_code, immigration, immigrant and demographics)</b>
        table | column_name | type | contains_null? | example_value | comment
        ---|---|---|---|---|---
        analytics_immigration | cicid | int | False | 40574 |
        analytics_immigration | age | int | False | 59 |
        analytics_immigration | occupation | str | True | STU |
        analytics_immigration | gender | str | False | M |
        analytics_immigration | monthYear | str | False | apr16 | month and year of the data
        analytics_immigration | from_country | str | False | UNITED KINGDOM' |
        analytics_immigration | state | str | False | ARIZONA |
        analytics_immigration | median_age | float | False | 35.0375 | median age of the group of people living the that city
        analytics_immigration | total_population | int | False | 22497710 |
        analytics_immigration | foreign_born | int | False | 3411565 | total number of people who are foreign born

# Tools and technologies used
- <b>Apache-airflow</b>
    - A very good tool for orchastracting data pipelines. Allows to backfill, schedule ETL pipelines. Open source
- <b>Spark on EMR</b>
    - As everyone knows spark is a great tool for processing big data. easy to scale up in case the data increases.
    - AWS EMR offeres good infrastructure and tool to run spark jobs
- <b>Livy</b>
    - Livy offers a great api to interact with spark cluster and to submit spark code. This way we do not need to convert spark (pyspark) code to .jar files. Shoter spark codes.
- <b>CloudFormation</b>
    - It is a very good feature offered by AWS to deploy a bunch of AWS resources at once in a form of stack.
    - Easy to maintain
    - Easy for redeployment in different environments dev, prod etc.
- <b>EC2</b>
    - Amazon's Elastic Compute 2 is a scalable solution to deploy Apache airflow
    - Always availability of airflow interface, no need to host on local machine

# Setting up AWS infrastructure
- To build a spark environment to run pyspark code I refered this [tutorial](https://aws.amazon.com/blogs/big-data/build-a-concurrent-data-orchestration-pipeline-using-amazon-emr-and-apache-livy/)
- A cloud formation script is given in the repository at `./cloud_formation_script/airflowCloudInfra.yaml`. Upload this script to create an EC2 instance to host Apache-Airflow, AWS database instance for postgres to store metadata of Airflow, security groups
- once the instance are created then connect to EC2 instance and run below code to trigger `immigration_analytics_pipeline` dag in airflow:
```
sudo su
source ~/.bash_profile
airflow initdb
airflow scheduler --daemon
airflow webserver --daemon -p 8080
airflow trigger_dag immigration_analytics_pipeline
```

# Flow of ETL pipeline in Airflow
- immigration_analytics_pipeline: 
    - Starts EMR cluster, then waits till the cluster goes into idle state
    - Livy API creates session and submits every data transforming scripts to EMR to perform operations on raw data
    - Once all the transformation and analytics tables are created then a quality check scripts runs
        - First quality check: Checks if all the data is available or not. Logs information accordingly.
        - Second quality check: Performs specified tests to cross verify expected and actual values. The tests are specified in `./dags/transform/check__unit_test.py`. Logs information about task if it failed or passed.
    - Once every operation is complete the pipeline terminates the cluster

# Possible scenarios    
- The data was increased by 100x.
    - If the data increases by 100x i would increase the number of executors in the spark cluster. Horizontal scaling along with a little bit vertical scaling would definitly increase the performance of spark.
    - I would also check for the shuffles happeing in spark to optimise it furhter for joins using Hive/spark bucketting technique. If the data is bucketted before storing it helps reduce shiffle intern saves lot of time and data movement across the netowrk.
    - Reduce the actions which cause the data to move to driver as the driver would get pretty loaded and become slow.
    - If dynamic shuffle is enabled then it would allows to dynamically create partitions, and parse or analyse the data in incremental manner, so this way we would only work with new data.
    - Usign `schedule_interval` option - more frequent runs less ammount of data to process


- The pipelines would be run on a daily basis by 7 am every day.
    - If the requirement is to run the pipeline by 7am on a daily basis, we can schedule the pipeline to run during night time, mostly midnight so that the latest data and updated data is available by 7 AM
    - Airflow allows us to schedule pipeline at a perticular time of the day using cron interval formating. As stated in scenario we can configure the pipeline to run by 7am by setting `schedule_interval= '0 2 * * *'`. This will run the pipeline at 2am ( or any feasible time based on the total time required to finish the pipeline ) and updated data will be ready to get updated on dashboard
    

- The database needed to be accessed by 100+ people.
    - The data can be stored in the form of paruet files on Amazon's s3. s3 is secure, durable and highly scalable place for parquet files. 
    - The parquet is a columnar data format which is designed to querieng large amounts of data across many users, regardless of is size, data processing framework, data model and programing language
    - The access can be log in based with read only access to raw data and if there any chagnes or new transformation tables being created by indivisual users then they can be given their own workspace (a dedicated directory on s3).
    - This is to avoid multiple users updating the same data.