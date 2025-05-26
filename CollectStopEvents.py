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


for vid in vehicleIds:
    try:
        print(f"\nFetching data for vehicle ID: {vid}")
        resp = requests.get(f"{baseUrl}{vid}")
        if resp.status_code == 404:
            print(f"404 ERROR for vehicle ID: {vid}")
            continue

        soup = BeautifulSoup(resp.text, 'html.parser')
        h2_tags = soup.find_all("h2")
        tables = soup.find_all("table")

        all_rows = []
        headers = None

        for h2, table in zip(h2_tags, tables):
            trip_id = h2.get_text(strip=True).split()[-1]
            rows = table.find_all("tr")

            if not rows:
                print(f"No rows found in table for trip_id: {trip_id}")
                continue

            # Only set headers once
            if not headers:
                # Try to find header cells, fallback if not present
                header_cells = rows[0].find_all("th") or rows[0].find_all("td")
                headers = [cell.get_text(strip=True) for cell in header_cells]
                headers.append("trip_id")

            # Loop through table rows (skip header)
            for row in rows[1:]:
                cells = [td.get_text(strip=True) for td in row.find_all("td")]
                if cells:
                    cells.append(trip_id)
                    all_rows.append(cells)

        if not all_rows:
            print(f"No valid data rows found for vehicle ID: {vid}")
            continue

        # Save JSON
        stops_df = pd.DataFrame(all_rows, columns=headers)
        stops_df.to_json(f"{path}/{datetime.date.today()}_{vid}.json", orient="records", indent=2)
        print(f"Saved data for vehicle ID: {vid}")

    except Exception as e:
        print(f"Error processing vehicle ID {vid}: {e}")