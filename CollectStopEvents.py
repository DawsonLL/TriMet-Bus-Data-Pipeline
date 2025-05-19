import datetime
import os
import pandas as pd
from bs4 import BeautifulSoup
import requests
import urllib.error as error


#our base Url
baseUrl = "https://busdata.cs.pdx.edu/api/getStopEvents?vehicle_num="
#reads vehicleids.txt and parses into a list for each id entry
vehicleIds = [line.strip() for line in open("vehicleids.txt","r").readlines()]

path = "./StopEvents/" + str(datetime.date.today())
os.makedirs(path, exist_ok=True)

for id in vehicleIds:
    
    resp = requests.get(f'{baseUrl}{id}')
    if resp.status_code == 404:
        print(f'404 ERROR for {id}')
    else:
        soup = BeautifulSoup(resp.text, 'html.parser')

        tops_df = pd.DataFrame
        tables = soup.find_all("table")
        headers = None

        all_rows = []
        for table in tables:
            rows = table.find_all("tr")
            if not headers:
                headers = [th.get_text(strip=True) for th in rows[0].find_all("th")]

            for row in rows[1:]:
                cells = [td.get_text(strip=True) for td in row.find_all("td")]
                if cells:
                    all_rows.append(cells)
        stops_df = pd.DataFrame(all_rows, columns=headers)
        
        stops_df.to_json(f'{path}/{datetime.date.today()}_{id}.json')
        

        