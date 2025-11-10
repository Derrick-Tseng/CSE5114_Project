import requests
import csv
import os

data_path = "/Users/derricktseng/Desktop/WashU/DE_Project/data/"
fips_csv_path = "/Users/derricktseng/Desktop/WashU/DE_Project/valid_fips_codes.csv"

city_code = []
with open(fips_csv_path, 'r') as f:
    reader = csv.DictReader(f)
    for row in reader:
        city_code.append(f"gdpall{row['fips_code']}")

print(f"Total FIPS codes to download: {len(city_code)}")
successful = 0
failed = 0
invalid_codes = []

for i, code in enumerate(city_code, 1):
    url = f"https://fred.stlouisfed.org/graph/fredgraph.csv?bgcolor=%23ebf3fb&chart_type=line&drp=0&fo=open%20sans&graph_bgcolor=%23ffffff&height=450&mode=fred&recession_bars=on&txtcolor=%23444444&ts=12&tts=12&width=1320&nt=0&thu=0&trc=0&show_legend=yes&show_axis_titles=yes&show_tooltip=yes&id={code}&scale=left&cosd=2001-01-01&coed=2023-01-01&line_color=%230073e6&link_values=false&line_style=solid&mark_type=none&mw=3&lw=3&ost=-99999&oet=99999&mma=0&fml=a&fq=Annual&fam=avg&fgst=lin&fgsnd=2020-02-01&line_index=1&transformation=lin&vintage_date=2025-11-02&revision_date=2025-11-02&nd=2001-01-01"
    dest_path = data_path + f"{code}.csv"

    try:
        with requests.get(url, stream=True, timeout=15) as r:
            r.raise_for_status()
            with open(dest_path, "wb") as f:
                for chunk in r.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
        successful += 1
        if i % 100 == 0:
            print(f"Progress: {i}/{len(city_code)} - Downloaded: {successful}, Failed: {failed}")
    except requests.exceptions.HTTPError:
        failed += 1
        invalid_codes.append(code.replace("gdpall", ""))
        if i % 100 == 0:
            print(f"Progress: {i}/{len(city_code)} - Downloaded: {successful}, Failed: {failed}")
    except Exception as e:
        print(f"Error downloading {code}: {e}")
        failed += 1
        invalid_codes.append(code.replace("gdpall", ""))

print(f"\nDownload complete")
print(f"Successfully downloaded: {successful}")
print(f"Failed/Invalid FIPS codes: {failed}")

if invalid_codes:
    print(f"\nRemoving {len(invalid_codes)} invalid FIPS codes from CSV")
    
    valid_codes = []
    with open(fips_csv_path, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row['fips_code'] not in invalid_codes:
                valid_codes.append(row['fips_code'])
    
    with open(fips_csv_path, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['fips_code'])
        for fips in valid_codes:
            writer.writerow([fips])
    
    print(f"Updated CSV with {len(valid_codes)} valid FIPS codes")
