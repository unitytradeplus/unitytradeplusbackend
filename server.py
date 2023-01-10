

from fastapi import FastAPI
from urllib.request import urlopen
import pandas as pd
import json
import re
from pandas import json_normalize

app = FastAPI()

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


@app.get('/')
async def root():

   dataframe = getUsdEvents();    # ------ output of step 01 , dataframe having usdevents
   dataframe = removeLowimpacts(dataframe)   # ------- output of step 02 
   dataframe = detectTrendSignal(dataframe)   #  -------- output of step 03 a
   dataframeList = separateGroups(dataframe)  #  ---------  output of (03)(b) , list contains dataframes
   dataframeList = findHighImpacts(dataframeList) #  ------- output of (04) & (05) , list contains dataframes
   dataframe = generateTimeIntervals(dataframeList) # ------- output of step 06 , dataframe having time/time intervals and signal, final output

   dataframeLastOutput = dataframe
    
   return [dataframeLastOutput.iloc[i].to_dict() for i in range(len(dataframeLastOutput))]


 

