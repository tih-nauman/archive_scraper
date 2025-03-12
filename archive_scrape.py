import requests

url = "https://archive.org/services/search/beta/page_production/?user_query=&page_type=collection_details&page_target=booksbylanguage_sanskrit&hits_per_page=100&page=3&aggregations=false"

payload = {}
headers = {
  'Accept-Language': 'en-US,en;q=0.9',
  'Connection': 'keep-alive',
  'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36',
#   'Cookie': 'PHPSESSID=en4tdfjd6qn6ahafso6k13ralg; abtest-identifier=e169f9fe0a74d2ef232d96a053f8cff1; donation-identifier=52515267bcec1afdcfaef6fa485292dc'
}

response = requests.request("GET", url, headers=headers, data=payload)

data = response.json()
