# -*- coding: utf-8 -*-
"""Movie Scraping.ipynb

Automatically generated by Colaboratory.

Original file is located at
    https://colab.research.google.com/drive/1vjUJpx-dNlFnr8SbK9fhDxZwIdOno08N
"""

import pandas as pd
import requests
import re
import numpy as np
from bs4 import BeautifulSoup

url_range = list(range(101, 1000, 100))
# print(url_range)

url = 'https://www.the-numbers.com/box-office-records/worldwide/all-movies/cumulative/all-time'

response = requests.get(url)
soup = BeautifulSoup(response.text, 'html.parser')
# print(soup)

links = []
for item in soup.find_all('tr'):
    links.append(item.find_all('a'))

# print(links[1][1])
link_array = np.array(links)
link_array = np.delete(link_array, 0)
print(link_array.shape)
link_array.reshape(50, 2)
print(link_array)
link_df = pd.DataFrame(link_array)
print(link_df)

# movies = soup.select('td.summary')
# print(movies)
list_check = []
for tr in soup.find_all('tr'):
    list_check.append(tr.get_text().split('\n'))

array = np.array(list_check)
array = array.reshape(101, 8)
print(np.size(array))
df = pd.DataFrame(array, index=array[:, 1])
new_header = df.iloc[0]
df = df[1:]
df.columns = new_header
df.columns = (
['Rank', 'Rank', 'Year', 'Movie', 'WorldwideBox_Office', 'DomesticBox_Office', 'InternationalBox_Office', 'Extra'])
print(df)
print(len(df))
exit(0)
# Commented out IPython magic to ensure Python compatibility.
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

# %matplotlib inline
no_sign_WW = []
no_sign_intl = []
no_sign_dom = []
index = 0
for item in df.WorldwideBox_Office:
    no_sign_WW.append(int(float(item[1:].replace(',', ''))))
    index += 1

index = 0
for item in df.DomesticBox_Office:
    number = item[1:].replace(',', '')
    # print(number)
'''    no_sign_dom.append(int(float(number)))
    index += 1'''

index = 0
for item in df.InternationalBox_Office:
    no_sign_intl.append(int(float(item[1:].replace(',', ''))))
    index += 1

df['no_sign_ww'] = no_sign_WW
df['no_sign_intl'] = no_sign_intl
# df['no_sign_dom'] = no_sign_dom

# print(df.no_sign_ww)
df = df.sort_values('Year')

plt.figure(figsize=(20, 10))
fig = plt.scatter(df.Year, df.no_sign_ww / 10000000)
# fig2 = plt.scatter(df.Year,df.no_sign_dom/10000000)
fig3 = plt.scatter(df.Year, df.no_sign_intl / 10000000)

plt.figure(figsize=(20, 10))

fig4 = plt.hist(df.Year)

import pandas as pd
import requests
import re
import numpy as np
from bs4 import BeautifulSoup

url_range = list(range(1, 1000, 100))
print(url_range)

url = 'https://www.the-numbers.com/box-office-records/worldwide/all-movies/cumulative/all-time/'
list_check = []
for index in url_range:
    response = requests.get(url + str(index))
    soup = BeautifulSoup(response.text, 'lxml')
    # soup = soup[1:]
    # print(soup)

    # links = soup.select('td.href')

    for tr in soup.find_all('tr'):
        list_check.append(tr.get_text().split('\n'))

array = np.array(list_check)
print(array)

# array = array.reshape(101,8)
'''
df = pd.DataFrame(array,index=array[:,1])
new_header = df.iloc[0]
df = df[1:]
df.columns = new_header
df
'''

# print(np.size(array))
print(array[101])
array = array.reshape(1010, 8)
# 0, 100, 201, 302, 403, 504, 605, 706, 807, 908 are headers
headers = df2.columns
print(headers)

for index in df2.iloc[:]:
    if index == headers:
        df2.iloc[headers].delete()

print(df2.iloc[100])
# for index in url_range[-1::-1]:
# print(index)
# print(df2.iloc[index-1])


df2 = pd.DataFrame(array, index=array[:, 1])
new_header = df2.iloc[0]
df2 = df2[1:]
df2.columns = new_header

# print('this index is' + df2.iloc[100])

url = 'https://www.the-numbers.com/movie/Avatar#tab=cast-and-crew'

response = requests.get(url)
soup2 = BeautifulSoup(response.text, 'lxml')
print(soup2)
