import os
import csv
from urllib.parse import urlparse

def extract_domain(url):
    try:
        parsed_url = urlparse(url)
        return parsed_url.netloc
    except:
        return None

def process_files(source_folder, output_file):
    domain_data = {}

    for file_name in os.listdir(source_folder):
        if file_name.endswith('.csv'):
            file_path = os.path.join(source_folder, file_name)
            with open(file_path, mode='r', newline='', encoding='utf-16') as csv_file:
                reader = csv.DictReader(csv_file, delimiter='\t')
                for row in reader:
                    domain = extract_domain(row['URL'])
                    domain_rating = row['Domain rating']
                    if domain and domain not in domain_data:
                        domain_data[domain] = domain_rating

    # Write the result to the output CSV file
    with open(output_file, mode='w', newline='', encoding='utf-8') as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow(["Domain", "Domain rating"])
        for domain, rating in domain_data.items():
            writer.writerow([domain, rating])

# Define the source folder and output file relative to the current working directory
source_folder = os.path.abspath(os.path.join(os.getcwd(), 'sources'))
output_file = os.path.abspath(os.path.join(os.getcwd(), 'result.csv'))

# Process the files
process_files(source_folder, output_file)
