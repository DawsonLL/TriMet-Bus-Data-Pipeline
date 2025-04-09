import urllib.request as request
import urllib.error as error
import datetime
import os

#our base Url
baseUrl = "https://busdata.cs.pdx.edu/api/getBreadCrumbs?vehicle_id="
#reads vehicleids.txt and parses into a list for each id entry
vehicleIds = [line.strip() for line in open("vehicleids.txt","r").readlines()]


for id in vehicleIds:
    url = baseUrl + id
    try:
        #retrieves the data for the assocaited ID and then download it into the Data Folder
        request.urlretrieve(url, f"Data/{datetime.date.today()}/{datetime.date.today()}_{id}.json")
    except error.HTTPError as e:
            print(f"{e} FOR {id}")

    