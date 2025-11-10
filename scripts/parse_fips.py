import re
import csv
import requests

def fetch_and_parse_fips():
    """Fetch and parse FIPS codes from FCC website."""
    url = "https://transition.fcc.gov/oet/info/maps/census/fips/fips.txt"
    
    try:
        response = requests.get(url)
        response.raise_for_status()
        text = response.text
        
        pattern = r'^\s*(\d{5})\s+(.+)$'
        
        fips_codes = []
        for line in text.split('\n'):
            match = re.match(pattern, line)
            if match:
                code = match.group(1)
                if not code.endswith('000'):
                    fips_codes.append(code)
        
        return fips_codes
    
    except Exception as e:
        print(f"Error fetching data: {e}")
        return []


def save_to_csv(fips_codes, output_file):
    """Save FIPS codes to CSV file."""
    with open(output_file, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['fips_code'])
        for code in sorted(fips_codes):
            writer.writerow([code])


if __name__ == "__main__":
    print("Fetching FIPS codes from FCC website")
    fips_codes = fetch_and_parse_fips()
    
    if fips_codes:
        output_file = "data/valid_fips_codes.csv"
        save_to_csv(fips_codes, output_file)
        print(f"{len(fips_codes)} county-level FIPS codes")
    else:
        print("No FIPS codes found")
