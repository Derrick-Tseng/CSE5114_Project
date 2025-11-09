#!/usr/bin/env python3
"""
Generate and update FIPS mapping with real county names from US Census Bureau API.
"""

import csv
import requests
import shutil
from pathlib import Path


STATE_NAMES = {
    '01': 'Alabama', '02': 'Alaska', '04': 'Arizona', '05': 'Arkansas',
    '06': 'California', '08': 'Colorado', '09': 'Connecticut', '10': 'Delaware',
    '11': 'District of Columbia', '12': 'Florida', '13': 'Georgia', '15': 'Hawaii',
    '16': 'Idaho', '17': 'Illinois', '18': 'Indiana', '19': 'Iowa',
    '20': 'Kansas', '21': 'Kentucky', '22': 'Louisiana', '23': 'Maine',
    '24': 'Maryland', '25': 'Massachusetts', '26': 'Michigan', '27': 'Minnesota',
    '28': 'Mississippi', '29': 'Missouri', '30': 'Montana', '31': 'Nebraska',
    '32': 'Nevada', '33': 'New Hampshire', '34': 'New Jersey', '35': 'New Mexico',
    '36': 'New York', '37': 'North Carolina', '38': 'North Dakota', '39': 'Ohio',
    '40': 'Oklahoma', '41': 'Oregon', '42': 'Pennsylvania', '44': 'Rhode Island',
    '45': 'South Carolina', '46': 'South Dakota', '47': 'Tennessee', '48': 'Texas',
    '49': 'Utah', '50': 'Vermont', '51': 'Virginia', '53': 'Washington',
    '54': 'West Virginia', '55': 'Wisconsin', '56': 'Wyoming', '72': 'Puerto Rico'
}


def fetch_county_names_from_census():
    """Fetch county names from US Census Bureau API."""
    url = "https://api.census.gov/data/2021/acs/acs5"
    params = {'get': 'NAME', 'for': 'county:*'}
    
    try:
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()
        
        county_mapping = {}
        for row in data[1:]:
            name, state_fips, county_fips = row[0], row[1], row[2]
            full_fips = f"{state_fips}{county_fips}"
            county_name = name.split(',')[0].strip()
            county_mapping[full_fips] = county_name
        
        return county_mapping
        
    except requests.exceptions.RequestException:
        print("Could not fetch from Census API.")


def update_fips_mapping_file(county_mapping, input_file, output_file):
    """Update FIPS mapping CSV with real county names. Skip unmappable FIPS codes."""
    skipped_count = 0
    written_count = 0
    
    with open(output_file, 'w', newline='', encoding='utf-8') as f_out:
        writer = csv.writer(f_out)
        writer.writerow(['fips_code', 'state_code', 'state_name', 'county_name', 'county_code'])
        
        with open(input_file, 'r', encoding='utf-8') as f_in:
            reader = csv.DictReader(f_in)
            
            for row in reader:
                fips_code = row['fips_code']
                state_code = fips_code[:2]
                state_name = STATE_NAMES.get(state_code, f'State {state_code}')
                
                # Only write rows that have real county mappings
                if county_mapping and fips_code in county_mapping:
                    county_name = county_mapping[fips_code]
                    county_code = fips_code[2:]
                    writer.writerow([fips_code, state_code, state_name, county_name, county_code])
                    written_count += 1
                else:
                    skipped_count += 1
                    print(f"Skipping unmappable FIPS code: {fips_code}")
    
    print(f"\nTotal written: {written_count}, Total skipped: {skipped_count}")
    return written_count, skipped_count


def main():
    project_root = Path(__file__).parent.parent
    input_file = project_root / 'data' / 'valid_fips_codes.csv'
    output_file = project_root / 'data' / 'fips_mapping.csv'
    
    if not input_file.exists():
        print(f"Error: Input file not found: {input_file}")
        return 1
    
    # Fetch real county names from Census API
    county_mapping = None
    county_mapping = fetch_county_names_from_census()
    
    # Write FIPS mapping file
    update_fips_mapping_file(county_mapping, input_file, output_file)
    
    print("FIPS mapping completed")
    return 0


if __name__ == '__main__':
    exit(main())
