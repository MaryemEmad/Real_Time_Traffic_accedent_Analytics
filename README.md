# Real_Time_Traffic_accedent_Analytics
A system simulating real-time accident data analysis. It uses Kafka to collect data, Spark for analysis, and stores the results in a  separate database.

# System Components

1.  __Traffic Data Producer__: Generates car traffic data and functions as a source for real-time information, streaming preprocessed US traffic accident data into a Kafka topic.
   
2.  __Consumer(Apache Spark Streaming)__: Consumes the data from the Kafka topic, processes it to identify Average Temperature and Humidity by State, Top 3 Cities with the Highest Accident Severity, and Accident Count by Weather Condition, storing results in databases.
   
3. __MySQL database__: Stores real-time insights in database named "big_data."
