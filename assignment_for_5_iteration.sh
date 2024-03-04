#!/bin/bash

# File: run_comparison.sh

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

# Specify the HiveQL query
HIVEQL_QUERY="SELECT Year, AVG((CarrierDelay / ArrDelay) * 100) AS AvgCarrierDelayPercentage FROM $HIVE_TABLE GROUP BY Year ORDER BY Year"

# Specify the Spark SQL query
SPARKSQL_QUERY="SELECT Year, AVG((CarrierDelay / ArrDelay) * 100) AS AvgCarrierDelayPercentage FROM $HIVE_TABLE GROUP BY Year ORDER BY Year"


# Run the comparison for 5 times
for i in {1..5}; do
    echo "Iteration $i"

    # Run the HiveQL query and measure the execution time
    HIVEQL_TIME=$( { time hive -e "$HIVEQL_QUERY"; } 2>&1 | grep real | awk '{print $2}' )
    echo "HiveQL Execution Time: $HIVEQL_TIME seconds"

    # Run the Spark SQL query and measure the execution time
    SPARKSQL_TIME=$( { time spark-sql -e "$SPARKSQL_QUERY"; } 2>&1 | grep real | awk '{print $2}' )
    echo "Spark SQL Execution Time: $SPARKSQL_TIME seconds"

    # Save the results to a CSV file
    echo "Iteration,HiveQL_Time,SparkSQL_Time" > execution_times.csv
    echo "$i, $HIVEQL_TIME, $SPARKSQL_TIME" >> execution_times.csv
done

# ...

# Plot the results using matplotlib in Python
python3 - <<EOF
import pandas as pd
import matplotlib
import numpy as np
matplotlib.use('Agg')  # Use Agg backend for non-interactive environments
import matplotlib.pyplot as plt

# Load the execution times from the CSV file
df = pd.read_csv('execution_times.csv', names=['Iteration', 'HiveQL_Time', 'SparkSQL_Time'])

# Set the width of the bars
bar_width = 0.35

# Set the positions for 'HiveQL' and 'Spark SQL' bars
positions = np.arange(len(df['Iteration']))

# Plot the clustered bar chart for execution times
plt.bar(positions - bar_width/2, df['HiveQL_Time'], bar_width, label='HiveQL')
plt.bar(positions + bar_width/2, df['SparkSQL_Time'], bar_width, label='Spark SQL', alpha=0.5)  # Use alpha to make bars semi-transparent
plt.xlabel('Iteration')
plt.ylabel('Execution Time (seconds)')
plt.title('HiveQL vs Spark SQL Query Execution Time')
plt.xticks(positions, df['Iteration'])
plt.legend()
plt.savefig('execution_times_clustered_plot.png')  # Save the plot as an image file