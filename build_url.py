import json
import os
from pathlib import Path

def collect_urls_from_json():
    # Directory containing JSON files of metadata
    json_dir = Path("/workspace/nfs/nauman/archive/booksbylanguage_urdu")
    urls = []
    
    # Iterate through all JSON files in the directory
    for json_file in json_dir.glob("*.jsonl"):
        try:
            with open(json_file, 'r', encoding='utf-8') as f:
                data = f.readlines()
                data = [json.loads(line) for line in data]
                # Check if data is a list or single item
                if isinstance(data, list):
                    items = data
                else:
                    items = [data]
                
                # Build URLs for each identifier
                for item in items:
                    if 'identifier' in item:
                        identifier = item['identifier']
                        url = f"https://archive.org/details/{identifier}"
                        urls.append(url)
        except Exception as e:
            print(f"Error processing {json_file}: {str(e)}")
    
    # Save all URLs to a text file
    output_file = "archive_urls_urdu.txt"
    with open(output_file, 'w', encoding='utf-8') as f:
        for url in urls:
            f.write(url + '\n')
    
    print(f"Total URLs collected: {len(urls)}")
    print(f"URLs saved to: {output_file}")

if __name__ == "__main__":
    collect_urls_from_json()