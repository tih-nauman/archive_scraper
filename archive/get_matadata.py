import requests
import json 



def fetch_aggregateion(aggregations_size=65535, aggregations="year", page_type="collection_details", collection="booksbylanguage_hindi", year=None):
  """
  Fetch data from the given URL using the provided headers, payload, and parameters.

  Args:
    url (str): The base URL to send the GET request to.
    headers (dict): The headers to include in the request.
    payload (dict): The payload to include in the request.
    params (dict): The query parameters to include in the request.

  Returns:
    requests.models.Response: The response object from the GET request.
  """
  
# Define base URL
  url = "https://archive.org/services/search/beta/page_production/"  
  
  
  # Define query parameters
  params = {
    "user_query": "",
    "page_type": page_type,
    "page_target": collection,
    "hits_per_page": 0,
    "aggregations": aggregations,
    "aggregations_size": aggregations_size,
  }
  
  if year:
    params["filter_map"] = json.dumps({"year": {str(year): "inc"}})
    
  response = requests.get(url, params=params)
  
  # print(response.json())
  
  
  return response.json()['response']['body']['aggregations'][aggregations]



# def fetch_data_with_params(
#   page =1,
#   hits_per_page = 1000,
#   page_type = "collection_details",
#   collection="booksbylanguage_hindi", years=None,
#   subjects=None):
#   """
#   Fetch data from the given URL using the provided headers, payload, and parameters.

#   Args:
#     base_url (str): The base URL to send the GET request to.
#     params (dict): The query parameters to include in the request.
#     headers (dict): The headers to include in the request.
#     payload (dict): The payload to include in the request.

#   Returns:
#     requests.models.Response: The response object from the GET request.
#   """
#   # Define parameters
#   params = {
#     "user_query": "",
#     "page_type": page_type,
#     "page_target": collection,
#     "hits_per_page": hits_per_page,
#     "page": page,
#     "aggregations": "false",
#   }

#   headers = {
#     'Accept': '*/*',
#     'Accept-Language': 'en-US,en;q=0.9',
#     'Connection': 'keep-alive',
#     'Sec-Fetch-Site': 'same-origin',
#     'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36'}

#   base_url =  "https://archive.org/services/search/beta/page_production/" 
   
#   filter_map = {}
#   if len(years) == 2:
#       start_year, end_year = years
#       filter_map ={
#           "year": {str(start_year): "gte", str(end_year): "lte"}
#       }
#   else:
#     filter_map = {
#         "year": {str(years[0]): "inc"}
#   }
      
#   if subjects:
#       subject_filter = {}
#       for subj in subjects:
#           subject_filter[subj] = "inc"
#       filter_map["subject"] = subject_filter
#   params["filter_map"] = json.dumps(filter_map)
      
#   response = requests.get(base_url, headers=headers, params=params)
  
#   return response



import requests
import json

def fetch_data_with_params(
    page=1,
    hits_per_page=1000,
    page_type="collection_details",
    collection="booksbylanguage_hindi", 
    years=None,
    subjects=None):
    """
    Fetch data from the Internet Archive API using the provided parameters.

    Args:
        page (int): The page number for pagination. Defaults to 1.
        hits_per_page (int): Number of results per page. Defaults to 1000.
        page_type (str): Type of page to search. Defaults to "collection_details".
        collection (str): The collection to search in. Defaults to "booksbylanguage_hindi".
        years (list): A list with start and end years [start_year, end_year] or a single year [year].
        subjects (list): A list of subjects to filter by.

    Returns:
        requests.models.Response: The response object from the GET request.
    """
    # Define base URL
    base_url = "https://archive.org/services/search/beta/page_production/"
    
    # Define parameters
    params = {
        "user_query": "",
        "page_type": page_type,
        "page_target": collection,
        "hits_per_page": hits_per_page,
        "page": page,
        "aggregations_size": 10,
    }
    
    # Set up headers
    headers = {
        'Accept': '*/*',
        'Accept-Language': 'en-US,en;q=0.9',
        'Connection': 'keep-alive',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'same-origin',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36',
        'sec-ch-ua': '"Chromium";v="134", "Not:A-Brand";v="24", "Google Chrome";v="134"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"'
    }
    
    # Create filter map based on parameters
    filter_map = {}
    
    # Add years filter if provided
    if years:
        if len(years) == 2:
            start_year, end_year = years
            filter_map["year"] = {str(start_year): "gte", str(end_year): "lte"}
        elif len(years) == 1:
            filter_map["year"] = {str(years[0]): ["gte","lte"]}
    
    # Add subjects filter if provided
    if subjects:
        subject_filter = {}
        for subj in subjects:
            subject_filter[subj] = "inc"
        filter_map["subject"] = subject_filter
    
    # Add filter_map to params if not empty
    if filter_map:
        params["filter_map"] = json.dumps(filter_map)
    # Make the request with retries
    max_retries = 3
    import time 
    retry_delay = 15  # seconds
    for attempt in range(max_retries):
        try:
            response = requests.get(base_url, headers=headers, params=params, timeout=30)
            response.raise_for_status()
            break
        except (requests.exceptions.Timeout, requests.exceptions.RequestException) as e:
            if attempt == max_retries - 1:
                print(f"Failed to fetch data after {max_retries} attempts.")
                return None
            time.sleep(retry_delay)
    
    return response



def get_bucket_aggregation(data):

    bucket_years = []
    temp_sum = 0 
    temp_dict = {}
    for year in data['buckets']:
        
        if temp_sum + year['doc_count'] >= 10000:    
            bucket_years.append({"total_sum": temp_sum, "data": temp_dict})
            temp_dict = {}
            temp_sum = 0
        temp_sum += year['doc_count']
        temp_dict[year['key']] = year['doc_count']
    
    return bucket_years
