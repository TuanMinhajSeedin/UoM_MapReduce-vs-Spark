#!/bin/bash

# Specify the CSV file and Hive table name
CSV_FILE="DelayedFlights-updated.csv"
HIVE_TABLE="delay_flights"

# Drop the existing Hive table if it exists
hive -e "DROP TABLE IF EXISTS $HIVE_TABLE;"

[ -e execution_times.csv ] && rm execution_times.csv
[ -e hive_count_result.csv ] && rm hive_count_result.csv

# Create Hive table from CSV file
hive -e "CREATE TABLE $HIVE_TABLE (
    Year INT,
    Month INT,
    DayofMonth INT,
    DayOfWeek INT,
    DepTime INT,
    CRSDepTime INT,
    ArrTime INT,
    CRSArrTime INT,
    UniqueCarrier STRING,
    FlightNum INT,
    TailNum STRING,
    ActualElapsedTime INT,
    CRSElapsedTime INT,
    AirTime INT,
    ArrDelay INT,
    DepDelay INT,
    Origin STRING,
    Dest STRING,
    Distance INT,
    TaxiIn INT,
    TaxiOut INT,
    Cancelled INT,
    CancellationCode STRING,
    Diverted INT,
    CarrierDelay INT,
    WeatherDelay INT,
    NASDelay INT,
    SecurityDelay INT,
    LateAircraftDelay INT
) 
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY ',' 
STORED AS TEXTFILE
LOCATION '/user/hive/warehouse/$HIVE_TABLE'
TBLPROPERTIES ('skip.header.line.count'='1')"

# Load data from CSV file into Hive table
hive -e "LOAD DATA LOCAL INPATH '$CSV_FILE' INTO TABLE $HIVE_TABLE;"

# Specify queries and mappings
QUERIES=(
    "SELECT Year, AVG((CarrierDelay / ArrDelay) * 100) AS AvgCarrierDelayPercentage FROM $HIVE_TABLE GROUP BY Year ORDER BY Year"
    "SELECT Year, AVG((NASDelay / ArrDelay) * 100) AS AvgNASDelayPercentage FROM $HIVE_TABLE GROUP BY Year ORDER BY Year"
    "SELECT Year, AVG((WeatherDelay / ArrDelay) * 100) AS AvgWeatherDelayPercentage FROM $HIVE_TABLE GROUP BY Year ORDER BY Year"
    "SELECT Year, AVG((LateAircraftDelay / ArrDelay) * 100) AS AvgLateAircraftDelayPercentage FROM $HIVE_TABLE GROUP BY Year ORDER BY Year"
    "SELECT Year, AVG((SecurityDelay / ArrDelay) * 100) AS AvgSecurityDelayPercentage FROM $HIVE_TABLE GROUP BY Year ORDER BY Year"
)

MAPPING=(
    "Career delay query"
    "Nas delay query"
    "Weather delay query"
    "Late aircraft delay query"
    "Security delay query"
)

# Initialize the CSV file for recording execution times with header
echo "Mapping,HiveQL_Time,SparkSQL_Time" > execution_times_table.csv

# Loop through queries
for ((index=0; index<${#QUERIES[@]}; index++)); do
    query="${QUERIES[$index]}"
    mapping="${MAPPING[$index]}"
    
    # Run the HiveQL query and measure the execution time
    HIVEQL_TIME=$( { time hive -e "$query"; } 2>&1 | grep real | awk '{print $2}' )
    echo "HiveQL Execution Time: $HIVEQL_TIME seconds"

    SPARKSQL_TIME=$( { time spark-sql -e "$query"; } 2>&1 | grep real | awk '{print $2}' )
    echo "SparkSQL Execution Time: $SPARKSQL_TIME seconds"

    # Append the results to the CSV file
    echo "$mapping,$HIVEQL_TIME,$SPARKSQL_TIME" >> execution_times_table.csv
done

