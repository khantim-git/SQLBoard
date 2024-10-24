def check_file_exists_by_name(directory_path, file_name):
    try:
        # List all files and directories in the given path
        files = dbutils.fs.ls(directory_path)
        
        # Check for the file by name
        for file_info in files:
            if file_info.name == file_name or file_info.name == file_name + '/':  # Also check for directories (with '/')
                print(f"File {file_name} exists in {directory_path}")
                return True
        
        # If the file is not found
        print(f"File {file_name} does not exist in {directory_path}")
        return False
    except Exception as e:
        print(f"Error: {e}")
        return False

# Example usage
directory_path = "adl://<your_account_name>.dfs.core.windows.net/<file_system>/<directory>"
file_name = "<your_file_name>"

file_exists = check_file_exists_by_name(directory_path, file_name)

if file_exists:
    print("File exists.")
else:
    print("File does not exist.")
