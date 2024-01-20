from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.functions import from_json
from pyspark.sql.types import StructType, StringType, IntegerType, TimestampType, FloatType
import pymysql


def insert_into_phpmyadmin(row):
    # Define the connection details for your PHPMyAdmin database
    host = "localhost"
    port = 3306
    database = "big_data"
    username = "root"
    password = ""

    conn = pymysql.connect(host=host, port=port, user=username, passwd=password, db=database)
    cursor = conn.cursor()

    # Extract the required columns from the row
    column1_value = row.name
    column2_value = row.age

    # Prepare the SQL query to insert data into the table
    sql_query = f"INSERT INTO user (name, age) VALUES ('{column1_value}', '{column2_value}')"

    # Execute the SQL query
    cursor.execute(sql_query)

    # Commit the changes
    conn.commit()
    conn.close()


# Create a Spark session
# Add the spark-sql-kafka dependency
spark = SparkSession.builder \
    .appName("Consumer") \
    .config("spark.jars", "C:/spark/spark-3.2.4-bin-hadoop2.7/spark-sql-kafka-0-10_2.12-3.2.4.jar") \
    .config("spark.jars", "C:/spark/spark-3.2.4-bin-hadoop2.7/jars/mysql-connector-j-8.2.0.jar")\
    .getOrCreate()
# Define the Kafka bootstrap servers and topic
kafka_bootstrap_servers = 'localhost:9092'
kafka_topic = 'demo2'

# Define the schema for the data


schema = StructType().add("ID", StringType()).add("Severity", IntegerType()).add("Start_Time",
TimestampType()).add("End_Time",TimestampType()).add("Street",StringType()).add("City",StringType()).add("State"
,StringType()).add("Country",StringType()).add("Timezone",StringType()).add("Weather_Timestamp",
TimestampType()).add("Temperature(F)",FloatType()).add("Wind_Chill(F)",FloatType()).add("Humidity(%)"
,IntegerType()).add("Pressure(in)",FloatType()).add("Visibility(mi)",IntegerType()).add("Wind_Direction"
,StringType()).add("Wind_Speed(mph)",FloatType()).add("Weather_Condition",StringType())


# Read data from Kafka topic as a DataFrame
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", kafka_topic) \
    .load() \
    .select(from_json(col("value").cast("string"), schema).alias("data")) \
    # .select("data.*")
# 1. Average Temperature and Humidity by State
avg_temp_humidity_by_state = df.groupBy("State").agg(avg("Temperature(F)").alias("Avg_Temperature"),
                                                     avg("Humidity(%)").alias("Avg_Humidity"))

# 2. Top 3 Cities with the Highest Accident Severity
top_10_cities_severity = df.groupBy("City").agg(avg("Severity").alias("Avg_Severity")) \
    .orderBy(desc("Avg_Severity")).limit(3)

# 3. Accident Count by Weather Condition
accident_count_by_weather = df.groupBy("Weather_Condition").agg(count("*").alias("Accident_Count")) \
    .orderBy(desc("Accident_Count"))

# Show the results
print("1. Average Temperature and Humidity by State:")
avg_temp_humidity_by_state.show()

print("2. Top 10 Cities with the Highest Accident Severity:")
top_10_cities_severity.show()

print("3. Accident Count by Weather Condition:")
accident_count_by_weather.show()


#chatgpt
import os
os.environ["HADOOP_HOME"] = "C:/hadoop"


# Wait for the query to finish
query.awaitTermination()

# Stop the Spark session
spark.stop()
