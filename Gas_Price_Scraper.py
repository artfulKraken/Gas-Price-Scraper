## Scrapes daily average gas price data from "https://gasprices.aaa.com/?state=CA#state-metro"

from bs4 import BeautifulSoup
from urllib.request import urlopen
import os
import datetime as DT
from operator import itemgetter
import requests
import certifi
import urllib3
import yaml
import logging
import time
import mysql.connector


###############################    FUNCTIONS    ##########################################
def importConfig():
  # imports yaml config file
  home_dir = os.path.expanduser('~')
  with open(home_dir + "/.gasScraperConfig.yml", "r") as file:
    config = yaml.safe_load(file)
  return config

def get_existing_csv_data(filepath):
  if os.path.isfile(filepath):
    with open(filepath, 'r') as file:
      lstFile= file.readlines()
      file.close()
    existGasData = []
    i = 0
    existGasDataHeaderRow = lstFile.pop(0)
    for line in lstFile:
      i+=1
      dictData = line.strip("\n").split(",")
      existGasData.append( {
        "date": dictData[0],
        "MetroArea": dictData[1],
        "regular": float( dictData[2].replace( '$', '' ) ),
        "mid": float( dictData[3].replace( '$', '' ) ),
        "premium": float( dictData[4].replace( '$', '' ) ),
        "dataAcc":int( dictData[5] ) 
      } )
    existGasData.sort(key=itemgetter("date","MetroArea"))
    lastDate = DT.date.fromisoformat(existGasData[-1]["date"])
    return ( existGasData, lastDate, existGasDataHeaderRow )

  else:
    ## If file doesnt exist, set lastDate to 7 days ago to use as a starting point for collecting data and existing data to empty list.
    lastDate = DT.date.today() - DT.timedelta(days=7) 
    existGasData = []  
    existGasDataHeaderRow = "Date,Region,Regular,Plus,Premium,Data Accuracy\n"
    return ( existGasData, lastDate, existGasDataHeaderRow )

def insertCsvData( existGasData, lstCAMetroPricing, filepath, existGasDataHeaderRow):
    # rewrites csv file with existing and new data
    try:
      existGasData.extend(lstCAMetroPricing)
    except:
      existGasData = []
      existGasData.extend(lstCAMetroPricing)

    f = open(filepath, "w")
    f.write(existGasDataHeaderRow)
    for dataline in existGasData:
      f.write(",".join([dataline["date"],dataline["MetroArea"],str(dataline["regular"]), str(dataline["mid"]), str(dataline["premium"]), str(dataline["dataAcc"])])+"\n")
    f.close()

def connect_to_mysql(config, attempts=3, delay=2):
    
  sql_config = {
    'user': config["mysql"]["user"],
    'password': config["mysql"]["password"],
    'host': config["mysql"]["host"],
    'port': config["mysql"]["port"],
    'database': config["mysql"]["database"],
    'raise_on_warnings': True
  }
  attempt = 1
  # Implement a reconnection routine
  while attempt < attempts + 1:
    try:
      return mysql.connector.connect(**sql_config)
    except (mysql.connector.Error, IOError) as err:
      if (attempts is attempt):
        # Attempts to reconnect failed; returning None
        logger.info("Failed to connect, exiting without a connection: %s", err)
        return None
      logger.info(
        "Connection failed: %s. Retrying (%d/%d)...",
        err,
        attempt,
        attempts-1,
      )
      # progressive reconnect delay
      time.sleep(delay ** attempt)
      attempt += 1
  return None

def getLastMySqlData( mySqlCnx ):
  cursor = mySqlCnx.cursor()

  query = ("SELECT date FROM gas_prices WHERE date = ( SELECT MAX(date) FROM gas_prices ) LIMIT 1")
  cursor.execute(query)

  for date in cursor:
    last_date = date

  cursor.close()
  return last_date[0]

def insertMySqlData( lstCAMetroPricing, config ):
  mySqlCnx = connect_to_mysql(config)
  if mySqlCnx == None:
    logger.error("Could not create a mysql database connection")
    exit(1)
  
  cursor = mySqlCnx.cursor()
  add_gas_price = ("INSERT INTO gas_prices (date, region, regular, plus, premium, accuracy) VALUES ( %s, %s, %s, %s, %s, %s )")

  for dataline in lstCAMetroPricing:
    dl = (dataline["date"] ,dataline["MetroArea"], dataline["regular"], dataline["mid"], dataline["premium"], dataline["dataAcc"])
    cursor.execute(add_gas_price, dl)
  
  mySqlCnx.commit()
  cursor.close()
  mySqlCnx.close()


def metroAreaGasPrice(MetroAreaHeaderSoup):
    MetroArea = MetroAreaHeaderSoup.next_sibling
    while MetroArea.name != "div":
        MetroArea = MetroArea.next_sibling
    MetroGPTableBody = MetroArea.find("tbody").find_all("tr")
    priceList = []
    for tablerow in MetroGPTableBody:
        tableData = tablerow.find_all("td")
    #####could use some additional error coding to ensure that no changes to format are found
        
        if tableData[0].get_text() == "Current Avg.":
            priceList.append({
                "MetroArea": h3.next,
                "regular": tableData[1].get_text()[1:],
                "mid": tableData[2].get_text()[1:],
                "premium": tableData[3].get_text()[1:]
            })
        
        elif tableData[0].get_text() == "Yesterday Avg.":
            priceList.append({
                "MetroArea": h3.next,
                "regular": tableData[1].get_text()[1:],
                "mid": tableData[2].get_text()[1:],
                "premium": tableData[3].get_text()[1:]
            })
        elif tableData[0].get_text() == "Week Ago Avg.":
            priceList.append({
                "MetroArea": h3.next,
                "regular": tableData[1].get_text()[1:],
                "mid": tableData[2].get_text()[1:],
                "premium": tableData[3].get_text()[1:]
            })
        elif tableData[0].get_text() == "Month Ago Avg.":
            priceList.append({
                "MetroArea": h3.next,
                "regular": tableData[1].get_text()[1:],
                "mid": tableData[2].get_text()[1:],
                "premium": tableData[3].get_text()[1:]
            })
        elif tableData[0].get_text() == "Year Ago Avg.":
            priceList.append({
                "MetroArea": h3.next,
                "regular": tableData[1].get_text()[1:],
                "mid": tableData[2].get_text()[1:],
                "premium": tableData[3].get_text()[1:]
            })
    return priceList

def extrapGasData(metroAreaData,curDate,lastDate):
    dataList = []
    daysBetween = (curDate-lastDate).days
    for day in range(1,daysBetween+1):
        if lastDate + DT.timedelta(days=day) == curDate:
            dataList.append({
                "date": (lastDate + DT.timedelta(days=day)).isoformat(),
                "MetroArea": metroAreaData[0]["MetroArea"],
                "regular": float(metroAreaData[0]["regular"]),
                "mid": float(metroAreaData[0]["mid"]),
                "premium": float(metroAreaData[0]["premium"]),
                "dataAcc": 0
            })
        elif lastDate + DT.timedelta(days=day) == curDate + DT.timedelta(days=-1):
            dataList.append({
                "date": (lastDate + DT.timedelta(days=day)).isoformat(),
                "MetroArea": metroAreaData[1]["MetroArea"],
                "regular": float(metroAreaData[1]["regular"]),
                "mid": float(metroAreaData[1]["mid"]),
                "premium": float(metroAreaData[1]["premium"]),
                "dataAcc": 0
            })
        elif lastDate + DT.timedelta(days=day) >= curDate + DT.timedelta(days=-7):
            dataList.append({
                "date": (lastDate + DT.timedelta(days=day)).isoformat(),
                "MetroArea": metroAreaData[2]["MetroArea"],
                "regular": round((lastDate+DT.timedelta(days=day) - (curDate+DT.timedelta(days=-7))).days*((float(metroAreaData[1]["regular"])-float(metroAreaData[2]["regular"]))/6)+float(metroAreaData[2]["regular"]),3),
                "mid": round((lastDate+DT.timedelta(days=day) - (curDate+DT.timedelta(days=-7))).days*((float(metroAreaData[1]["mid"])-float(metroAreaData[2]["mid"]))/6)+float(metroAreaData[2]["mid"]),3),
                "premium": round((lastDate+DT.timedelta(days=day) - (curDate+DT.timedelta(days=-7))).days*((float(metroAreaData[1]["premium"])-float(metroAreaData[2]["premium"]))/6)+float(metroAreaData[2]["premium"]),3),
                "dataAcc": 1
            })
        elif lastDate + DT.timedelta(days=day) >= curDate + DT.timedelta(days=-30):
            dataList.append({
                "date": (lastDate + DT.timedelta(days=day)).isoformat(),
                "MetroArea": metroAreaData[3]["MetroArea"],
                "regular": round((lastDate+DT.timedelta(days=day) - (curDate+DT.timedelta(days=-30))).days*((float(metroAreaData[2]["regular"])-float(metroAreaData[3]["regular"]))/23)+float(metroAreaData[3]["regular"]),3),
                "mid": round((lastDate+DT.timedelta(days=day) - (curDate+DT.timedelta(days=-30))).days*((float(metroAreaData[2]["mid"])-float(metroAreaData[3]["mid"]))/23)+float(metroAreaData[3]["mid"]),3),
                "premium": round((lastDate+DT.timedelta(days=day) - (curDate+DT.timedelta(days=-30))).days*((float(metroAreaData[2]["premium"])-float(metroAreaData[3]["premium"]))/23)+float(metroAreaData[3]["premium"]),3),
                "dataAcc": 2
            })
        elif lastDate + DT.timedelta(days=day) >= curDate + DT.timedelta(days=-365):
            dataList.append({
                "date": (lastDate + DT.timedelta(days=day)).isoformat(),
                "MetroArea": metroAreaData[4]["MetroArea"],
                "regular": round((lastDate+DT.timedelta(days=day) - (curDate+DT.timedelta(days=-365))).days*((float(metroAreaData[3]["regular"])-float(metroAreaData[4]["regular"]))/335)+float(metroAreaData[4]["regular"]),3),
                "mid": round((lastDate+DT.timedelta(days=day) - (curDate+DT.timedelta(days=-365))).days*((float(metroAreaData[3]["mid"])-float(metroAreaData[4]["mid"]))/335)+float(metroAreaData[4]["mid"]),3),
                "premium": round((lastDate+DT.timedelta(days=day) - (curDate+DT.timedelta(days=-365))).days*((float(metroAreaData[3]["premium"])-float(metroAreaData[4]["premium"]))/335)+float(metroAreaData[4]["premium"]),3),
                "dataAcc": 3
            })
    return dataList



###############################  End FUNCTIONS  ##########################################

### MAIN #####

#import yaml config file
config = importConfig()

# Set up logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")

# Log to console
handler = logging.StreamHandler()
handler.setFormatter(formatter)
logger.addHandler(handler)

# log to a file
file_handler = logging.FileHandler( config[ "logging" ][ "filepath" ] )
file_handler.setFormatter(formatter)
logger.addHandler(file_handler) 

GasPriceURL = config["general"]["GasPriceURL"]

#### get existing data from csv or most recent data from mysql, depending on config settings.  exit if incorrect output value (not csv or mysql)

if config["general"]["output"] == 'mysql':
  mySqlCnx = connect_to_mysql(config)
  if mySqlCnx == None:
    logger.error("Could not create a mysql database connection")
    exit(1)
  lastDate = getLastMySqlData( mySqlCnx )
  mySqlCnx.close()

elif config["general"]["output"] == 'csv':
  tupLastData = get_existing_csv_data( config["csv"]["filepath"] )
  existGasData = tupLastData[0]
  lastDate = tupLastData[1]
  existGasDataHeaderRow = tupLastData[2]

else:
  logger.error("Invalid config output option")
  exit(1)


http = urllib3.PoolManager(
    cert_reqs="CERT_REQUIRED",
    ca_certs=certifi.where()
)
headers = urllib3.HTTPHeaderDict()
headers.add('User-Agent','Mozilla/5.0')
resp = http.request("GET", GasPriceURL,headers=headers)

#html = urlopen(resp).read()
soup = BeautifulSoup(resp.data,'html.parser')


dateSoup = soup.find("p",class_="price-text price-text--blue").next_sibling
while dateSoup.name !="p":
    dateSoup = dateSoup.next_sibling
dateString = dateSoup.get_text()
dateString = dateString.split(" ")[-1].split("/")

curDate = DT.date(int("20"+dateString[2]),int(dateString[0]),int(dateString[1]))


if curDate != lastDate:
  lstCAMetroPricing =[]
  MetroAccordian = soup.find_all(attrs={"class": "accordion-prices metros-js"})
  MetroAreas = MetroAccordian[0].find_all("h3")
  for h3 in MetroAreas:
    #if h3.next == "Orange County":
    metroAreaData = metroAreaGasPrice(h3)
    lstCAMetroPricing.extend(extrapGasData(metroAreaData,curDate,lastDate))


  lstCAMetroPricing.sort(key=itemgetter("date","MetroArea"))

  if config["general"]["output"] == 'mysql':
    insertMySqlData( lstCAMetroPricing, config )
  elif config["general"]["output"] == 'csv':
    insertCsvData( existGasData, lstCAMetroPricing, config["csv"]["filepath"], existGasDataHeaderRow )
