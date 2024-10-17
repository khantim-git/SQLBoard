from pyspark.sql import SparkSession
from datetime import datetime
import time

# Initialize a Spark session
spark = SparkSession.builder.getOrCreate()

# Define the loop condition
x = None

# Loop until x becomes 10 or current time > 12 PM
while True:
    # Check the current time
    current_time = datetime.now()
    
    # Break the loop if the current time is past 12 PM
    if current_time.hour >= 12:
        print(f"Current time is {current_time.strftime('%H:%M')}, breaking the loop (past 12 PM).")
        break
    
    # Replace 'y' with your actual table name or query
    query = "SELECT x FROM y LIMIT 1"
    
    # Execute the SQL query
    result = spark.sql(query)
    
    # Extract the value of x
    x = result.collect()[0]['x']
    
    # Print the value of x
    print(f"Current value of x: {x}")
    
    # Break the loop if x is 10
    if x == 10:
        print("x is 10, breaking the loop.")
        break
    
    # Sleep for 30 minutes (1800 seconds) before the next iteration
    print("Waiting for 30 minutes before the next query...")
    time.sleep(1800)
