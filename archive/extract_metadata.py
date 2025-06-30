from get_metadata import fetch_data_with_params, fetch_aggregateion, get_bucket_aggregation
import time 
import json
import os 

FOLDER = "metadata_archive_sanskrit"
COLLLECTION = "booksbylanguage_sanskrit"

def get_data(years, subjects = None):
    data = []
    
    max_p = min(11, years['total_sum']//1000 + 2)
    year =None
    for page in range(1, max_p):
        
        if len(years['data']) !=0:
            if len(years['data']) ==1:
                year = str(list(years['data'].keys())[0])
                start_year=  year
                end_year = year
                year = [year]
            else:
                start_year=  min(years['data'].keys())
                end_year = max(years['data'].keys())
                year = [str(start_year), str(end_year)]

        print(f"Fetching data for page {page}", year)
        response = fetch_data_with_params(years=year, subjects=subjects, page=page)
        
        if response.status_code != 200:
            print(f"Error fetching data for page {page}")
            continue
        data.extend(response.json()['response']['body']['hits']['hits'])
        time.sleep(2)
        if not data[0]:
            return "fata"
        
    return data


def save_to_jsonl(filename,
                  data):
    
    os.makedirs(FOLDER, exist_ok=True)
    
    # --
    
    
    filename = os.path.join(FOLDER, filename)
    
    with open(filename, 'a', encoding='utf-8') as f:
        for item in data:
            f.write(f"{json.dumps(item)}\n")    

if __name__ == "__main__":

    years_agg = fetch_aggregateion(collection=COLLLECTION, aggregations="year")
    years_agg['buckets']  = sorted(years_agg['buckets'], key=lambda x: x['key'], reverse=True)
    years_agg_buckets = get_bucket_aggregation(years_agg)
    
    extra_data_year = []
    
    final_data = []
    t_sum = 0
    for year in years_agg_buckets:
        
        years = [str(k) for k in year['data'].keys()]
        year_str = f"{years[0]}-{years[-1]}"
        file_name = f"metadata_archive_{year_str}.jsonl"
        
        if ( os.path.exists(os.path.join(FOLDER,file_name)) or 
            os.path.exists(os.path.join(FOLDER,f"metadata_archive_{"-".join(years)}.jsonl"))
        ):
            print(f"Data already fetched for {year_str}")
            continue
        
        if year['total_sum'] > 10000:
            extra_data_year.append(year)
            continue
        
        data = get_data(year)
        
        # year_str = '-'.join(str(k) for k in year['data'].keys())
        print(f"Data fetched for Expected :: ", year['total_sum'])
        print(f"Data fetched for {year_str} :: Unique :: {len(set(i['fields']['identifier'] for i in data))}  ::-> count", len(data))
        save_to_jsonl(file_name, data)
        
        final_data.extend(data)
        t_sum += year['total_sum']
        print(f"Data fetched for Expected :: ", t_sum)
        print(f"Data fetched for TOtal :: Unique :: {len(set(i['fields']['identifier'] for i in final_data))}  ::-> count", len(final_data))

        time.sleep(100)
    print("All date wise done ")
    
    # Extracting Extra data on years
    for year_bucket in extra_data_year:
        
        year = str(list(year_bucket['data'].keys())[0])
        subjects_agg = fetch_aggregateion(collection=COLLLECTION, aggregations="subject", year=year)
        subjects_agg_buckets = get_bucket_aggregation(subjects_agg)
        DATA = []
        for subject_bucket in subjects_agg_buckets:
            subjects = list(subject_bucket['data'].keys())
            data = get_data(year_bucket, subjects=subjects)
            print(f"Data fetched for {subjects} expected {subject_bucket['total_sum']} for year {year} :: ", len(data))
            DATA.extend(data)
        save_to_jsonl(f"metadata_archive_{year}.jsonl", DATA)
        
    # if True:
    #     subjects_agg = fetch_aggregateion(collection=COLLLECTION, aggregations="subject", year=None)
    #     subjects_agg_buckets = get_bucket_aggregation(subjects_agg)
    #     DATA = []
    #     for subject_bucket in subjects_agg_buckets:
    #         subjects = list(subject_bucket['data'].keys())
    #         if subject_bucket['total_sum'] ==0:
    #             continue
    #         year_bucket = {'total_sum': subject_bucket['total_sum'], 
    #                        'data': []}
    #         data = get_data(year_bucket, subjects=subjects)
    #         print(f"Data fetched for {subjects} expected {subject_bucket['total_sum']}  :: ", len(data))
    #         # DATA.extend(data)
    #         subject= "-".join(subjects[:2])
    #         save_to_jsonl(f"metadata_archive_{subject}.jsonl", data)
