import requests
from bs4 import BeautifulSoup

url = "https://dumps.wikimedia.org/backup-index.html"

response = requests.get(url)
soup = BeautifulSoup(response.text, 'html.parser')


# find xpath /html/body/div/ul/li/a 
soup.find_all('/html/body/div/ul/li')

# find all a tags
a_tags = soup.find_all('a')

# print all a tags
for a in a_tags:
    
    print(a.text)

#  write into a text file
with open('a_tags.txt', 'w') as f:
    for a in a_tags:
        f.write(a.text + '\n')

# print all a tags with href
for a in a_tags:
    print(a['href'])

# write into a text file
with open('dumps.txt', 'w') as f:
    for a in a_tags:
        f.write(a['href'] + '\n')