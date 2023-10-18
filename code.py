import json
import pandas as pd
import requests
from bs4 import BeautifulSoup
#import re
import boto3
import time

where=13616368

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3"
}






def keywords(id):
    word=''
    key_url='https://www.imdb.com/title/tt'+id+'/keywords/'
    #https://www.imdb.com/title/tt'+id_+'/'
    response_keyword=requests.get(key_url,headers=headers)
    content_keyword=response_keyword.content
    soup_keyword=BeautifulSoup(content_keyword,'html.parser')
    keyword_elements=soup_keyword.find_all('a',class_='ipc-metadata-list-summary-item__t')
    for element in keyword_elements:
     word += ', ' + element.text
    #print('keyword')
    return word[2:len(word)]

#release date function
def release_info(soup):
   date='NaN'
   x= soup.find_all('li',class_='ipc-metadata-list__item ipc-metadata-list-item--link')
   for r in x:
      if 'Release date' in r.text:
       date=r.text.replace('Release date','')
       break
   return date

# runtime fuction
def runtime(soup):
 runtime_test=soup.find_all('li',class_='ipc-metadata-list__item')
 color='NaN'
 runtime="NaN"
 for i in runtime_test:
    if 'Runtime' in i.text:
     runtime=i.text.replace('Runtime','')   
    if 'Color' in i.text:
      color=i.text #.replace('Color',' ')
 return runtime,color
      
    
#Country fuction

def country(soup):
    country_name='NaN'
    for t in soup.find_all('li', class_='ipc-metadata-list__item'):
     if 'Country of origin' in t.text:
      country_name=t.text.replace('Country of origin','')


    return country_name

movie_genres = ["Action","Adventure","Comedy","Drama","Horror","Science Fiction (Sci-Fi)","Fantasy","Romance","Mystery","Crime","Thriller","Animation","Musical","War","Western","Biography","Historical",
    "Documentary","Family","Sports","Superhero","Musical Comedy","Film Noir","Romantic Comedy (Rom-Com)","Fantasy Adventure","Action Comedy","Psychological Thriller","Science Fantasy",
    "Epic","Post-Apocalyptic","History"]   

def genere_fun(soup):
 gen_list=' '
 try:
  genres = soup.find_all('span', class_='ipc-chip__text')
  for g in  genres:
    if g.text in movie_genres:
        gen_list=g.text+' '+gen_list
  return gen_list 
 except:
  return 'NaN'

def cast(soup):
    div_elements=soup.find_all('a',class_='sc-bfec09a1-1 fUguci')
    list=[element.text for element in div_elements]
    if len(list)>4:
        return list[0:6]
    else:
     return list

    

df={'Id':[],'Title':[],'Storyline':[],'Genres':[],'Director':[],'Cast':[],'Date':[],'Runtime':[],'Rating':[],'Color':[],'Country':[],'keywords':[]}

i=1
start_time=time.time()
for num in range(where,where-50000,-1):
      len_num=7-len(str(num))
      id_='0'*len_num+str(num)
      w_url='https://www.imdb.com/title/tt'+id_+'/'
      try:
       response=requests.get(w_url,headers=headers)
       content=response.content
       soup=BeautifulSoup(content,'html.parser')
       Title=soup.find('span',class_='sc-afe43def-1 fDTGTb').text
      except:
       print(i,w_url,'>> Error')
       continue
      print(i,w_url,'<< processing')
      Title=soup.find('span',class_='sc-afe43def-1 fDTGTb').text
      try:
       Director=soup.find_all('a',class_='ipc-metadata-list-item__list-content-item ipc-metadata-list-item__list-content-item--link')[0].text
      except:
        Director=' '
      Date=release_info(soup)
 
      Runtime_,colr=runtime(soup)
      try:
        Rating=(soup.find_all('span',class_='sc-bde20123-1 iZlgcd')[0]).text
      except:
       Rating="NaN"
      country_=country(soup)
      df['Country'].append(country_)
      key_words=keywords(str(id_))
      i=i+1
      df['Id'].append(id_)
      df['Link'].append(w_url)
      df['Title'].append(Title)
      df['Cast'].append(cast(soup))
      df['Director'].append(Director)
      df['Genres'].append(genere_fun(soup))
      df['Date'].append(Date)
      df['Runtime'].append(Runtime_)
      df['Color'].append(colr)
      df['Rating'].append(Rating)
      df['keywords'].append(key_words)
    
main_df=pd.DataFrame(df)
    
csv_buffer = StringIO()
main_df.to_csv(csv_buffer, index=False)
csv_data = csv_buffer.getvalue() 

s3 = boto3.client('s3')
# Specify the S3 bucket and object key

bucket_name = 'imdbucket'
object_key = 'imdb2.csv'

    # Upload the data to S3
s3.put_object(Bucket=bucket_name, Key=object_key, Body=scraped_data)


