import csv
import os
import requests

# Common function to extract stats from Ahrefs API
def fetch_ahrefs_data(url_template, domain, cookie):
    url = url_template.replace("DOMAIN_PLACEHOLDER", domain)
    
    headers = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'Accept-Encoding': 'gzip, deflate, br, zstd',
        'Accept-Language': 'en-US,en;q=0.9,uk;q=0.8',
        'Cache-Control': 'max-age=0',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36 Edg/126.0.0.0',
        'Cookie': cookie
    }

    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        return response.json()[1]
    else:
        print(f"Failed to fetch data for {domain}: {response.status_code}")
        return {}

# Function to process the output file and fetch Ahrefs stats
def process_output_file(output_file, cookie):
    temp_output_file = output_file + ".tmp"

    with open(output_file, mode='r', newline='', encoding='utf-8') as infile:
        reader = csv.DictReader(infile)
        fieldnames = reader.fieldnames + [
            'backlinks_current', 'backlinks_current_delta', 'backlinks_all_time',
            'refdomains_current', 'refdomains_current_delta', 'refdomains_all_time',
            'ahrefs_rank', 'ahrefs_rank_delta', 'domain_rating', 'domain_rating_delta'
        ]
        
        # Write headers to the temp output file
        with open(temp_output_file, mode='w', newline='', encoding='utf-8') as outfile:
            writer = csv.DictWriter(outfile, fieldnames=fieldnames)
            writer.writeheader()

    # Process each row and write to the temp output file
    with open(output_file, mode='r', newline='', encoding='utf-8') as infile:
        reader = csv.DictReader(infile)
        
        for row in reader:
            domain = row['Domain']
            
            # Check if domain has already been processed
            if len([value for value in row.values() if value]) > 2:
                with open(temp_output_file, mode='a', newline='', encoding='utf-8') as outfile:
                    writer = csv.DictWriter(outfile, fieldnames=fieldnames)
                    writer.writerow(row)
                continue

            backlinks_url_template = 'https://app.ahrefs.com/v4/seBacklinksStats?input=%7B%22args%22%3A%7B%22competitors%22%3A%5B%5D%2C%22best_links_filter%22%3A%22showAll%22%2C%22backlinksFilter%22%3Anull%2C%22country%22%3A%22us%22%2C%22compareDate%22%3A%5B%22Ago%22%2C%22Week%22%5D%2C%22multiTarget%22%3A%5B%22Single%22%2C%7B%22protocol%22%3A%22both%22%2C%22mode%22%3A%22subdomains%22%2C%22target%22%3A%22DOMAIN_PLACEHOLDER%2F%22%7D%5D%2C%22url%22%3A%22DOMAIN_PLACEHOLDER%2F%22%2C%22protocol%22%3A%22both%22%2C%22mode%22%3A%22subdomains%22%7D%7D'
            domain_rating_url_template = 'https://app.ahrefs.com/v4/seGetDomainRating?input=%7B%22args%22%3A%7B%22competitors%22%3A%5B%5D%2C%22best_links_filter%22%3A%22showAll%22%2C%22backlinksFilter%22%3Anull%2C%22country%22%3A%22us%22%2C%22compareDate%22%3A%5B%22Ago%22%2C%22Week%22%5D%2C%22multiTarget%22%3A%5B%22Single%22%2C%7B%22protocol%22%3A%22both%22%2C%22mode%22%3A%22subdomains%22%2C%22target%22%3A%22DOMAIN_PLACEHOLDER%2F%22%7D%5D%2C%22url%22%3A%22DOMAIN_PLACEHOLDER%2F%22%2C%22protocol%22%3A%22both%22%2C%22mode%22%3A%22subdomains%22%7D%7D'
            
            backlinks_stats = fetch_ahrefs_data(backlinks_url_template, domain, cookie)
            domain_rating_stats = fetch_ahrefs_data(domain_rating_url_template, domain, cookie)
            
            row.update({
                'backlinks_current': backlinks_stats.get('backlinks', {}).get('current', {}).get('value', ''),
                'backlinks_current_delta': backlinks_stats.get('backlinks', {}).get('current', {}).get('delta', ''),
                'backlinks_all_time': backlinks_stats.get('backlinks', {}).get('all_time', ''),
                'refdomains_current': backlinks_stats.get('refdomains', {}).get('current', {}).get('value', ''),
                'refdomains_current_delta': backlinks_stats.get('refdomains', {}).get('current', {}).get('delta', ''),
                'refdomains_all_time': backlinks_stats.get('refdomains', {}).get('all_time', ''),
                'ahrefs_rank': domain_rating_stats.get('ahrefsRank', {}).get('value', ''),
                'ahrefs_rank_delta': domain_rating_stats.get('ahrefsRank', {}).get('delta', ''),
                'domain_rating': domain_rating_stats.get('domainRating', {}).get('value', ''),
                'domain_rating_delta': domain_rating_stats.get('domainRating', {}).get('delta', '')
            })
            
            with open(temp_output_file, mode='a', newline='', encoding='utf-8') as outfile:
                writer = csv.DictWriter(outfile, fieldnames=fieldnames)
                writer.writerow(row)

    # Replace the original output file with the new file
    os.replace(temp_output_file, output_file)

# Define the output file path
output_file = os.path.abspath(os.path.join(os.getcwd(), 'result.csv'))

# Read the cookie from an external file
with open('cookie.txt', 'r') as cookie_file:
    cookie = cookie_file.read().strip()

# Process the output file to fetch and append Ahrefs stats
process_output_file(output_file, cookie)
