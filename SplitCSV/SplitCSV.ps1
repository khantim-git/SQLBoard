param (
    # Path to the large CSV file (input)
    [string]$inputFile,

    # Number of lines per smaller file
    [int]$linesPerFile = 1000,  # Default is 1000 if not provided

    # Directory to save the output files (default is the same as input file directory)
    [string]$outputDirectory
)

# Check if input file exists
if (-Not (Test-Path $inputFile)) {
    Write-Host "The input file does not exist at the specified path."
    exit
}

# If output directory is not provided, set it to the input file's directory
if (-Not $outputDirectory) {
    $outputDirectory = [System.IO.Path]::GetDirectoryName($inputFile)
}

# Read all the data from the large CSV file
$csvData = Import-Csv -Path $inputFile

# Calculate the total number of small files needed
$totalFiles = [math]::Ceiling($csvData.Count / $linesPerFile)

# Loop through and split the data into smaller files
for ($i = 0; $i -lt $totalFiles; $i++) {
    # Calculate the starting and ending line numbers for each chunk
    $startLine = $i * $linesPerFile
    $endLine = [math]::Min(($i + 1) * $linesPerFile, $csvData.Count)
    
    # Extract the chunk of data for the current small file
    $chunk = $csvData[$startLine..($endLine - 1)]
	
	$baseName = [System.IO.Path]::GetFileNameWithoutExtension($inputFile)
    
    # Create the output file path for the current small file
    $outputFile = Join-Path -Path $outputDirectory -ChildPath ($baseName+"_$($i + 1).csv")
    
    # Export the chunk of data into a new CSV file
    $chunk | Export-Csv -Path $outputFile -NoTypeInformation
}

# Print a message indicating how many small files have been created
Write-Host "Successfully split into $totalFiles small files."
