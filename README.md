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


# Data model and design
## Data Model
- In this project I will be using a Star model
    - Easey to implement and understand
    - flexible

## Table designs and table dictionary
1. In this i have created a total of 9 tables out of which 
    - 3 are reference tables for City, Country and State
    - 4 are dimmension tables creating using reference and raw data
    - 2 are facts tables for analytics
2. <b>Reference Tables</b>
    1. <b>city (using airport and demographic data)</b>
        1. city_id
        2. city
        3. state_code
    2. <b>country_code (from i94 immigration data description file)</b>
        1. code
        2. country
    3. <b>state_code (from i94 immigration data description file)</b>
        1. code
        2. state
3. <b>Dimmension table</b>
    1. <b>airports (From data sources #4)</b>
        1. icao_code: airport ident code
        2. type: airport type
        3. name: airport name
        4. elevation_ft: elevation of airport from sea level
        5. iata_code: international airport code
        6. airport_latitude: location [latidute]
        7. airport_longitude: location [longitude]
        8. city_id: From city table in reference tanle
    2. <b>demographics (From data sources #3)</b>
        1. male_population: total male population
        2. female_population: total female population
        3. total_population: total population
        4. num_veterans: total number of vaterans
        5. foreign_born: total number of people who are foreign borm
        6. avg_household_size: on an average the size of a family
        7. race: race
        8. median_age: median age of the group of people living the that city
        9. city_id
    3. <b>weather</b>
        1. avg_temp: average temperature of the city
        2. std_temp: average uncertainty in the temperature of the city
        3. longitude
        4. latitude
        5. city_id
    4. <b>immigrants ( from i94 immigration data)</b>
        1. cicid: immigrants reference id
        2. from_country_code: the country where the immigrants came from
        3. age: age
        4. visa_code: reason of travel
        5. visa_post: the department of state where the visa was issued
        6. occupation: occupation of immigrant
        7. visa_type: type of visa issue
        8. birth_year: year of birth
        9. gender
        10. monthYear: month and year of the data arrival
    5. <b>immigration (from i94 immigration data)</b>
        1. cicid: immigrants reference id
        2. admnum: admission number
        3. iata_code: international airport code
        4. state_code
        5. airline: name of the airline used by the immigrant for travel
        6. fltno: flight number
        7. entdepa: arrival flag
        8. entdepd: departure flag
        9. entdepu: updated flag
        10. matflag: match flag
        11. arrival_date: date of arrival
        12. departure_date: date of departure
        13. deadline_departure: deadline date of departure for corrosponding immigrant
        14. monthYear: month and year of the data
3. <b>Analytics table (facts)</b>
    1. <b>airport analytics (using transformed city, airport, weather, state_code and country code data)</b>
        1. name: name of airport
        2. elevation_ft: elevation of airport location from sea level in ft.
        3. city: name of the city
        4. state: name of the state
        5. avg_temp: average temperature at the location
        6. std_temp: aaverage uncertainty in the temperature at the location
        7. longitude
        8. latitude
    2. <b>immigration analytics (using transformed city, state_code, country_code, immigration, immigrant and demographics)</b>
        1. cicid: immigrants reference id
        2. age: age
        3. occupation: occupation of immigrant
        4. gender
        5. monthYear: month and year of the data
        6. from_country_code: the country where the immigrants came from
        7. state: name of the state
        8. median_age: median age of the group of people living the that city
        9. total_population: total population
        10. foreign_born: total number of people who are foreign borm

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
    - Once every operation is complete the pipeline terminates the cluster

# Possible scenarios    
- Data increase by 100x. read > write. write > read
    - Increate the nodes on EMR cluster
    - Better use of partitioning and bucketting techniques in spark


- Pipelines would be run on 7am daily. how to update dashboard? would it still work?
    - Schedule the pipeline to run everyday at 7 AM. Configure to allow retries to accomodate errors due to memory issues or network issue.
    - configure Airflow to send emails on task, dag fails so as to fix them as early as possible
    - send emails when tests or checks fail


- Make it available to 100+ people
    - Redshift with auto-scaling capabilities and good read performance
    - Cassandra with pre-defined indexes to optimize read queries
