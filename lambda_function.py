import json
import pandas as pd
import requests
from bs4 import BeautifulSoup
#import re
import boto3
import time

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3"
}

s3 = boto3.client('s3')
bucket_name='imdbucket'
file_key = 'imdb.csv'
#path='D:\Jovian data\data analysis projects\IMDB\imdb.csv'
obj = s3.get_object(Bucket=bucket_name, Key=file_key)
csv_content = obj['Body'].read()
print(csv_content)
#df = pd.read_csv(io.BytesIO(csv_content))


try:
    #load_df = pd.read_csv(path,sep=';')
    load_df=pd.read_csv(io.BytesIO(csv_content))
    where=load_df['Id'].iloc[-1]+1
except:
  where=1
print(where)
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

def release_info(id):
    key_url='https://www.imdb.com/title/tt'+id+'/releaseinfo/'
    #https://www.imdb.com/title/tt'+id_+'/'
    response_release_info=requests.get(key_url,headers=headers)
    content_release_info=response_release_info.content
    soup_release_info=BeautifulSoup(content_release_info,'html.parser')
    date=soup_release_info.find('span',class_='ipc-metadata-list-item__list-content-item')
    try:
       soup_release_info.find('article',class_='sc-8eb80a87-0 hQQXzj').text is not None
       #print('release info')
       return 'NaN'

    except:
       #print('release info')
       return date.text


def runtime(id):
    key_url='https://www.imdb.com/title/tt'+id+'/technical/'
    #https://www.imdb.com/title/tt'+id_+'/'
    response_runtime=requests.get(key_url,headers=headers)
    content_runtime=response_runtime.content
    soup_runtime=BeautifulSoup(content_runtime,'html.parser')
  
    runtime=soup_runtime.find('li',class_='ipc-metadata-list__item').text
    try:
     color=soup_runtime.find_all('a',class_='ipc-metadata-list-item__list-content-item ipc-metadata-list-item__list-content-item--link')[1].text
    except:
      color='NaN'
    
    if runtime is not []:
     if 'Runtime' in runtime:
      runtime=runtime.replace('Runtime','') 
      #print('runtime')
      return runtime ,color
    else:
      #print('runtime')
      runtime
    return 'NaN',color 
    
#Country fuction

def country(soup):
    r_items = soup.find_all('a', class_='ipc-metadata-list-item__list-content-item ipc-metadata-list-item__list-content-item--link')
    inde=0
    for r in r_items:

      if r.text == 'None':
        country_name= r_items[inde-1].text
        break
      else:
        country_name='NaN'
      inde=inde+1
    #print('country')
    return country_name



df={'Id':[],'Link':[],'Title':[],'Storyline':[],'Director':[],'Date':[],'Runtime':[],'Rating':[],'Color':[],'Country':[],'keywords':[]}

def lambda_handler(event, context):
    i=1
    start_time=time.time()
    for num in range(where,100000000):
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
                              storyline=soup.find_all('div',class_='ipc-html-content-inner-div')[2].text
                             except:
                               storyline='NaN'
                             Director=soup.find_all('a',class_='ipc-metadata-list-item__list-content-item ipc-metadata-list-item__list-content-item--link')[0].text
                             Date=release_info(id_)
 
                             Runtime_,colr=runtime(id_)
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
                             df['Storyline'].append(storyline)
                             df['Director'].append(Director)
                             df['Date'].append(Date)
                             df['Runtime'].append(Runtime_)
                             df['Color'].append(colr)
                             df['Rating'].append(Rating)
                             df['keywords'].append(key_words)
                             if i==100:
                               try:
                                main_df=pd.DataFrame(df)
                                save_df=pd.concat([load_df, main_df], ignore_index=True)
                                save_df.to_csv(path,index=False,sep=';',header=True)
                                load_df = pd.read_csv(path,sep=';')
                                print('>>',load_df.shape)
                                df={'Id':[],'Link':[],'Title':[],'Storyline':[],'Director':[],'Date':[],'Runtime':[],'Rating':[],'Color':[],'Country':[],'keywords':[]}
                                end_time=time.time()
                                print('>>Time', (end_time-start_time)/100)
                                start_time=time.time()
                               except:
                                 main_df=pd.DataFrame(df)
                                 save_df=main_df
                                 save_df.to_csv(path,index=False,sep=';',header=True)
                                 load_df = pd.read_csv(path,sep=';')
                                 print('>>',load_df.shape)
                                 df={'Id':[],'Link':[],'Title':[],'Storyline':[],'Director':[],'Date':[],'Runtime':[],'Rating':[],'Color':[],'Country':[],'keywords':[]}
                                 print('File not found Solved')
 
                               i=1
    
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
