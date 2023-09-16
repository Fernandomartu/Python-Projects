import os
import requests
import csv

# Path to the CSV file
csv_file_path = "tier1.csv"

# Folder where the PDF files will be saved
output_folder = "downloadedpdfs"

# Number of retries
num_retries = 3

# Open the CSV file
with open(csv_file_path, 'r') as file:
    reader = csv.reader(file)
    next(reader)  # Skip the header row
    
    # Iterate over each row in the CSV file
    for index, row in enumerate(reader):
        pdf_url = row[52]  # Assuming the URL is in the 53rd column (zero-based index)
        
        filename = pdf_url.rsplit('/', 1)[-1]  # Extract the filename from the URL
        output_path = os.path.join(output_folder, filename)
        
        # Retry downloading the PDF file
        for retry in range(num_retries):
            try:
                response = requests.get(pdf_url, timeout=10)  # Set timeout value here (in seconds)
                if response.status_code == 200:
                    with open(output_path, 'wb') as f:
                        f.write(response.content)
                    print(f"Downloaded file {index+1}")
                    break  # Break out of the retry loop if successful
                else:
                    print(f"Failed to download file {index+1}")
            except requests.exceptions.RequestException as e:
                print(f"Failed to download file {index+1} - Retry {retry+1}/{num_retries}")
                print(f"Error: {str(e)}")