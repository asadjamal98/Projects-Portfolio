import requests
import pandas as pd
from bs4 import BeautifulSoup

# Set URL of website to import data from
URL = "https://en.wikipedia.org/wiki/List_of_highest-grossing_films"

# Request URL and save response
page = requests.get(URL)

# Create BeautifulSoup object containing all data
data = BeautifulSoup(page.text, 'html.parser')

# Store desired table and table data
tbl = data.find('table')
tbl_data = tbl.find_all('tr')

# Create header list
tbl_header_tags = tbl_data[0]
tbl_header_titles = [data.text.strip() for data in tbl_header_tags if data.text.strip()]

# Store all row data as a list
list_of_data = []
for index, row in enumerate(tbl_data[1:]):
    tbl_row = [data.text.strip() for data in row.find_all('td')]
    tbl_row.insert(2, row.find('a').text if row.find('a') else None)
    list_of_data.append(tbl_row)
# Create the main dataframe
df = pd.DataFrame(list_of_data, columns= tbl_header_titles)

# Removing Reference column as it is not needed
del df['Ref']

# Save to a csv
df.to_csv('Highest_Grossing_Films.csv', index=False)
