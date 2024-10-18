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
    try:
        full_path = uri + filepath
        dbutils.fs.put(full_path, content, True)
        print(f"File written to: {full_path}")
    except Exception as e:
        print(f"Error writing to ADLS: {e}")

# Function to check if today is a working day by querying the `working_day` table
def is_working_day():
    try:
        # Get today's date in 'YYYY-MM-DD' format
        today_date = datetime.now().strftime('%Y-%m-%d')
        
        # Query the working_day table to see if today is a working day
        query = f"SELECT work_date FROM working_day WHERE work_date = '{today_date}'"
        
        # Execute the query
        result = spark.sql(query)
        
        # Check if today's date is in the result
        return result.count() > 0
    except Exception as e:
        print(f"Error checking working day: {e}")
        return False

# Function to check the value of x from the table 'y'
def get_x_value():
    try:
        query = "SELECT x FROM y LIMIT 1"
        result = spark.sql(query)
        return result.collect()[0]['x']
    except Exception as e:
        print(f"Error querying value of x: {e}")
        return None

# Function to check if today is the 2nd day of the month
def is_second_day_of_month():
    return datetime.now().day == 2

# Main execution
if is_working_day():
    while True:
        # Check current time
        current_time = datetime.now()
        
        # Break if current time is after 12 PM
        if current_time.hour >= 12:
            print(f"Current time is {current_time.strftime('%H:%M')}, breaking the loop (past 12 PM).")
            break
        
        # Get the value of x
        x = get_x_value()
        
        # If x is None, skip this loop iteration (likely a query error)
        if x is None:
            print("Failed to retrieve the value of x. Skipping this iteration.")
            continue
        
        # Print the current value of x
        print(f"Current value of x: {x}")
        
        # If x == 0, write to ADLS and break
        if x == 0:
            print("x is 0, writing to ADLS and breaking the loop.")
            write_to_adls(adl_uri, filepath + filename_suffix, "x = 0")
            
            # Check if today is the 2nd day of the month, if yes, write additional file
            if is_second_day_of_month():
                monthly_filename = f"Monthly_{datetime.now().strftime('%Y%m')}.txt"
                write_to_adls(adl_uri, filepath + monthly_filename, "Monthly report")
            
            break
        
        # Sleep for 30 minutes before the next query
        print("Waiting for 30 minutes before the next query...")
        time.sleep(1800)
else:
    print("Today is not a working day. Program will not run.")
