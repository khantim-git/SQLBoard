# Function to check if the current date is the first day of the month
def is_first_day_of_month():
    # Get the current date
    current_date = datetime.now()
    
    # Return True if the day is the 1st, False otherwise
    return current_date.day == 1
