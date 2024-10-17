from pyspark.sql import SparkSession
from datetime import datetime
import time

# Initialize Spark session
spark = SparkSession.builder.getOrCreate()

# Define ADLS path (replace with actual ADLS URI and path)
adl_uri = "adl://<your-adl-uri>"
filepath = "/<your-path>/tocken_"
filename_suffix = datetime.now().strftime('%Y%m%d') + ".txt"

# Function to write x = 0 to ADLS
def write_to_adls(uri, filepath, content):
    full_path = uri + filepath
    dbutils.fs.put(full_path, content, True)
    print(f"File written to: {full_path}")

# Function to check if today is a working day by querying the `working_day` table
def is_working_day():
    # Get today's date in 'YYYY-MM-DD' format
    today_date = datetime.now().strftime('%Y-%m-%d')
    
    # Query the working_day table to see if today is a working day
    query = f"SELECT work_date FROM working_day WHERE work_date = '{today_date}'"
    
    # Execute the query
    result = spark.sql(query)
    
    # Check if today's date is in the result
    return result.count() > 0

# Loop until x becomes 0, or until 12 PM, only if today is a working day
if is_working_day():
    while True:
        # Check current time
        current_time = datetime.now()
        
        # Break if current time is after 12 PM
        if current_time.hour >= 12:
            print(f"Current time is {current_time.strftime('%H:%M')}, breaking the loop (past 12 PM).")
            break
        
        # Replace 'y' with your actual table name
        query = "SELECT x FROM y LIMIT 1"
        
        # Execute the SQL query
        result = spark.sql(query)
        
        # Extract the value of x
        x = result.collect()[0]['x']
        
        # Print the current value of x
        print(f"Current value of x: {x}")
        
        # If x == 0, write to file and break
        if x == 0:
            print("x is 0, writing to ADLS and breaking the loop.")
            write_to_adls(adl_uri, filepath + filename_suffix, "x = 0")
            break
        
        # Sleep for 30 minutes before the next query
        print("Waiting for 30 minutes before the next query...")
        time.sleep(1800)
else:
    print("Today is not a working day. Program will not run.")
