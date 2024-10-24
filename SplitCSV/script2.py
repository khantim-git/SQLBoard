# Function to check if a specific file exists by its name in a given directory
def check_file_exists_by_name(directory_path, file_name):
    try:
        # List all files and directories within the provided directory path
        files = dbutils.fs.ls(directory_path)
        
        # Loop through the list of files and directories
        for file_info in files:
            # Check if the file name matches (also check if it's a directory by adding '/')
            if file_info.name == file_name or file_info.name == file_name + '/':  
                print(f"File {file_name} exists in {directory_path}")
                return True
        
        # If the loop finishes and no matching file is found
        print(f"File {file_name} does not exist in {directory_path}")
        return False
    except Exception as e:
        # Handle any errors (e.g., the directory does not exist)
        print(f"Error: {e}")
        return False

# Example usage
directory_path = "adl://<your_account_name>.dfs.core.windows.net/<file_system>/<directory>"  # Path to the directory
file_name = "<your_file_name>"  # The file name you want to check

file_exists = check_file_exists_by_name(directory_path, file_name)  # Check if the file exists

if file_exists:
    print("File exists.")
else:
    print("File does not exist.")
