from datetime import datetime, timedelta

def is_today_4th_working_day():
    """
    Checks if the current date is the 4th working day of the current month.
    """
    try:
        # Get today's date in 'YYYY-MM-DD' format
        today_date = datetime.now().strftime('%Y-%m-%d')

        # Get the first and last day of the current month
        first_day_of_month = datetime.now().strftime('%Y-%m-01')
        last_day_of_month = datetime.now().replace(day=28) + timedelta(days=4)  # Ensure we get the last day
        last_day_of_month = last_day_of_month.strftime('%Y-%m-%d')

        # Query to get all working days in the current month
        query = f"""
        SELECT work_date 
        FROM working_day
        WHERE work_date >= '{first_day_of_month}' 
          AND work_date <= '{last_day_of_month}'
        ORDER BY work_date ASC
        """

        # Execute the query and collect the result
        result = spark.sql(query).collect()

        # Check if there are at least 4 working days
        if len(result) >= 4:
            # Get the 4th working day (index 3 in 0-based index)
            fourth_working_day = result[3]['work_date']

            # Check if today's date matches the 4th working day
            if today_date == fourth_working_day:
                return True  # Today is the 4th working day
            else:
                return False  # Today is not the 4th working day
        else:
            return False  # Not enough working days in the current month
    except Exception as e:
        return f"Error checking if today is the 4th working day: {e}"
