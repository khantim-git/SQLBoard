def get_4th_working_day_of_current_month():
    """
    Returns the 4th working day of the current month.
    """
    try:
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
            fourth_working_day = result[3]['work_date']  # The 4th working day (index 3 in 0-based index)
            return f"The 4th working day of the current month is: {fourth_working_day}"
        else:
            return f"There are less than 4 working days in the current month."
    except Exception as e:
        return f"Error retrieving the 4th working day: {e}"
