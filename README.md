# IMDB
 The code appears to be a Python script for web scraping IMDb movie data and storing it in a Pandas DataFrame, which is then converted to a CSV file and uploaded to an AWS S3 bucket. Here's a detailed explanation of each part of the code:

Importing Libraries:
python
Copy code
import json
import pandas as pd
import requests
from bs4 import BeautifulSoup
import boto3
import time
These lines import necessary Python libraries. requests is used for making HTTP requests, BeautifulSoup is used for web scraping, pandas is used for working with data, and boto3 is used for interacting with AWS services.

# Defining Constants:

where = 13616368
where is the starting IMDb ID for the movies to be scraped.

User-Agent Header:

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3"
}

This defines a user-agent header to mimic a web browser when making requests to IMDb. This can help avoid being blocked by anti-scraping measures.

Scraping Functions:
The code defines several functions to extract information from IMDb movie pages. These functions include keywords, release_info, runtime, country, genre_fun, and cast. Each function extracts specific details like keywords, release date, runtime, etc., from an IMDb page.

# Initializing DataFrame:

```df = {'Id': [], 'Title': [], 'Storyline': [], 'Genres': [], 'Director': [], 'Cast': [], 'Date': [], 'Runtime': [],'Rating': [], 'Color': [], 'Country': [], 'Keywords': []}```
This initializes an empty dictionary df that will be used to create a Pandas DataFrame to store scraped data.

# Main Scraping Loop:
for num in range(where, where - 50000, -1):
    # ...
    # The main loop iterates through a range of IMDb IDs, sending requests to IMDb for each movie's webpage.
    # It then extracts information using the defined scraping functions and appends it to the DataFrame.
#Creating DataFrame and CSV:

main_df = pd.DataFrame(df)
csv_data = main_df.to_csv(index=False)
This part creates a Pandas DataFrame from the scraped data and converts it to a CSV format.

# Uploading to AWS S3:

s3 = boto3.client('s3')
bucket_name = 'imdbucket'
object_key = 'imdb2.csv'
s3.put_object(Bucket=bucket_name, Key=object_key, Body=csv_data)
Here, the script uses the boto3 library to interact with AWS S3. It uploads the CSV data to an S3 bucket named 'imdbucket' with the object key 'imdb2.csv'.

# Execution Time Calculation:
end_time = time.time()
execution_time = end_time - start_time
print(f"Execution Time: {execution_time} seconds")
The code calculates and prints the execution time of the entire script.

Summary:
In summary, this code is a Python script that scrapes IMDb movie data, including details like movie titles, cast, release dates, and more, for a range of IMDb IDs. It stores this data in a Pandas DataFrame, converts it to a CSV file, and uploads it to an AWS S3 bucket. This script can be useful for collecting IMDb movie data for analysis or research purposes.
