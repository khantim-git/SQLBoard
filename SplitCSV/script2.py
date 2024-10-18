def generate_earliest_working_day_within_n_days(n):
    """
    Generates the earliest working day within n days before the current date.
    """
    try:
        # Get today's date in 'YYYY-MM-DD' format
        today_date = datetime.now().strftime('%Y-%m-%d')

        # Query to get the earliest working day within n days before the current date
        query = f"""
        SELECT MIN(work_date) as earliest_work_date
        FROM working_day
        WHERE work_date < '{today_date}'
        ORDER BY work_date DESC
        LIMIT {n}
        """

        # Execute the query and collect the result
        result = spark.sql(query).collect()

        # Check if we found any working days
        if result and result[0]['earliest_work_date'] is not None:
            earliest_working_day = result[0]['earliest_work_date']  # Get the earliest day
            return f"The earliest working day within {n} days before {today_date} is: {earliest_working_day}"
        else:
            return f"No working days found within {n} days before {today_date}."
    except Exception as e:
        return f"Error generating working day content: {e}"
