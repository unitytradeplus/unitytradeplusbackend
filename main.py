import requests
from itertools import zip_longest
from bs4 import BeautifulSoup
from fastapi import FastAPI, HTTPException, Request
from fastapi.templating import Jinja2Templates
from fastapi.encoders import jsonable_encoder
from fastapi.responses import HTMLResponse
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd
import json
import re
from pandas import json_normalize
from urllib.request import urlopen


app = FastAPI()
templates = Jinja2Templates(directory="templates")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


URL = 'https://www.tradingview.com/symbols/BTCUSDT/ideas/?sort=recent&video=no'
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'}



@app.get("/")
def root(request:Request):
    return templates.TemplateResponse("index.html",{"request":request})


def GROUPER(iterable, n, fillvalue=None):
    "Collect data into fixed-length chunks or blocks"
    # grouper('ABCDEFG', 3, 'x') --> ABC DEF Gxx
    args = [iter(iterable)] * n
    return zip_longest(*args, fillvalue=fillvalue)

@app.get("/advice", response_class=HTMLResponse)
async def advice(request:Request):
    RES = requests.get(URL, headers=HEADERS)
    if RES.status_code != 200:
        raise HTTPException(status_code=500, detail="Fetch failed!")
    SOUP = BeautifulSoup(RES.content, 'lxml')
    DIVS = SOUP.find_all(
        'div', class_='tv-widget-idea js-userlink-popup-anchor')
    IDEAS = []
    for DIV in DIVS:
        try:
            TITLE = DIV.find('div', class_='tv-widget-idea__title-row').text
            IMAGE = DIV.find('picture').find('img').get('data-src')
            AUTHOR = DIV.find('span', class_='tv-card-user-info__name')
            DETAIL = DIV.find('p').text
            IDEAS.append({
                "TITLE": TITLE.replace('\t', '').replace('\n', ''),
                "IMAGE": IMAGE,
                "AUTHOR": AUTHOR.text,
                "DETAIL": DETAIL.replace('\t', '').replace('\n', '').replace('-','')
            })
        except:
            raise HTTPException(status_code=500, detail="Parse failed!")
    return templates.TemplateResponse("advice.html",{"request":request,"ideas":list(GROUPER(IDEAS,3))})


@app.get("/advice/api")
async def data():
    RES = requests.get(URL, headers=HEADERS)
    if RES.status_code != 200:
        raise HTTPException(status_code=500, detail="Fetch failed!")
    SOUP = BeautifulSoup(RES.content, 'lxml')
    DIVS = SOUP.find_all(
        'div', class_='tv-widget-idea js-userlink-popup-anchor')
    IDEAS = []
    for DIV in DIVS:
        try:
            TITLE = DIV.find('div', class_='tv-widget-idea__title-row').text
            IMAGE = DIV.find('picture').find('img').get('data-src')
            AUTHOR = DIV.find('span', class_='tv-card-user-info__name')
            DETAIL = DIV.find('p').text
            IDEAS.append({
                "TITLE": TITLE.replace('\t', '').replace('\n', ''),
                "IMAGE": IMAGE,
                "AUTHOR": AUTHOR.text,
                "DETAIL": DETAIL.replace('\t', '').replace('\n', '')
            })
        except:
            raise HTTPException(status_code=500, detail="Parse failed!")
    return jsonable_encoder(list(GROUPER(IDEAS,3)))


def getUsdEvents():       #  -------- to get dataframe having usdevents     ------------(01)
  url = "https://economic-calendar.deta.dev/"
  
  # store the response of URL
  response = urlopen(url)
    
  # storing the JSON response 
  # from url in data
  data_json = json.loads(response.read())
    
  # print the json response
  # print(data_json)

  dataframe = json_normalize(data_json) 
  return dataframe     # ---------------- dataframe having usdevents

def removeLowimpacts(dataframe):   # to remove events having "low" impact  ------------(02)
  dataframe = dataframe[ (dataframe['impact'] == "medium") | (dataframe['impact'] == "high")]     
  return dataframe 

def detectTrendSignal(dataframe):  #to detect trend signals   ----------------(03)(a)
  dataframe['signal'] = None

  for x in range(0,len(dataframe.index)):
    if((dataframe.iloc[x, 4] != "") and (dataframe.iloc[x, 5] != "")):
      previous = re.findall(r"[-+]?(?:\d*\.*\d+)", dataframe.iloc[x, 4])   #  value in string type
      consensus = re.findall(r"[-+]?(?:\d*\.*\d+)", dataframe.iloc[x, 5])  #  value in string type

      previous = float(previous[0])   #  value in float type
      consensus = float(consensus[0])   #  value in float type

      if(consensus<previous):
        dataframe.iloc[x,7] = "Buy"
      elif(previous<consensus):
        dataframe.iloc[x,7] = "Sell"
      else:
        dataframe.iloc[x,7] = "Neutral"
      
    else:
      dataframe.iloc[x, 7] = None  

  return dataframe 

def separateGroups(dataframe_):   # to separate groups  ---------------(03)(b)          
  y = 0
  groupList = [pd.DataFrame()]
  groupList[0] = groupList[0].append(dataframe_.iloc[0,:], ignore_index=True)
  for x in range(1,len(dataframe_.index)):
    if(dataframe_.iloc[x,7]==dataframe_.iloc[x-1,7]):
      groupList[y] = groupList[y].append(dataframe_.iloc[x,:], ignore_index=True)   
    else:
      groupList.append(pd.DataFrame())
      y += 1
      groupList[y] = groupList[y].append(dataframe_.iloc[x,:], ignore_index=True)
  return groupList     # return list contains dataframes

def findHighImpacts(list):    # check whether previous groups have at least one high impact event & separate those groups ----------------(04) & (05)
  groupList = []
  for i in list:
    for x in range(0,len(i.index)):
      if(i.iloc[x,3]=="high"):
        groupList.append(i)
        break
  return groupList    # return list contains dataframes

def generateTimeIntervals(list):    # generate time/time interval of those groups made in previous step   ------------------------(06)
  df = pd.DataFrame(columns=['from_time','to_time','signal']) 
  for i in list:
    array =[]   # create new array as new row to "df" dataframe
    from_time = None
    to_time = None
    # dic_ =	{}   
    # dic_["signal"] = i.iloc[0,11]
    if(len(i.index)==1):
      from_time = i.iloc[0,2]
    else:  
      from_time = i.iloc[0,2]
      to_time = i.iloc[len(i.index)-1,2]

    array.append(from_time)
    array.append(to_time)
    array.append(i.iloc[0,7])
    df.loc[len(df.index)] = array  # add new row into "df" dataframe
    # df = df.append(dic_, ignore_index=True)    add new row into "df" dataframe
  
  return df    # return dataframe having time/time intervals and signal  


@app.get('/news',response_class=HTMLResponse)
async def news(request:Request):
   dataframe = getUsdEvents();    # ------ output of step 01 , dataframe having usdevents
   dataframe = removeLowimpacts(dataframe)   # ------- output of step 02 
   dataframe = detectTrendSignal(dataframe)   #  -------- output of step 03 a
   dataframeList = separateGroups(dataframe)  #  ---------  output of (03)(b) , list contains dataframes
   dataframeList = findHighImpacts(dataframeList) #  ------- output of (04) & (05) , list contains dataframes
   dataframe = generateTimeIntervals(dataframeList) # ------- output of step 06 , dataframe having time/time intervals and signal, final output

   dataframeLastOutput = dataframe
    
   news =  [dataframeLastOutput.iloc[i].to_dict() for i in range(len(dataframeLastOutput))]
   
   return templates.TemplateResponse("news.html",{"request":request,"signals":news})


@app.get('/news/api',response_class=HTMLResponse)
async def news_Api(request:Request):
   dataframe = getUsdEvents();    # ------ output of step 01 , dataframe having usdevents
   dataframe = removeLowimpacts(dataframe)   # ------- output of step 02 
   dataframe = detectTrendSignal(dataframe)   #  -------- output of step 03 a
   dataframeList = separateGroups(dataframe)  #  ---------  output of (03)(b) , list contains dataframes
   dataframeList = findHighImpacts(dataframeList) #  ------- output of (04) & (05) , list contains dataframes
   dataframe = generateTimeIntervals(dataframeList) # ------- output of step 06 , dataframe having time/time intervals and signal, final output

   dataframeLastOutput = dataframe
    
   news =  [dataframeLastOutput.iloc[i].to_dict() for i in range(len(dataframeLastOutput))]
   
   return news


