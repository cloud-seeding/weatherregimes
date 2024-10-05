"""
REQUIREMENTS:

requests
pandas
geopandas
"""

import os
import multiprocessing
from functools import partial
from datetime import datetime, timedelta

import requests
import pandas as pd
import geopandas as gpd


def get_relevant_months(event_date):
    year_months = []
    if not isinstance(event_date, datetime):
        event_date = datetime.strptime(event_date, "%Y-%m-%d")

    year_months.append(event_date.strftime("%Y%m"))

    if event_date.day <= 2:
        previous_month = (event_date.replace(day=1) -
                          timedelta(days=1)).strftime("%Y%m")
        year_months.append(previous_month)

    next_month = event_date.replace(day=28) + timedelta(days=4)
    last_day = next_month - timedelta(days=next_month.day)
    if event_date.date() >= last_day.date() - timedelta(days=1):
        next_month = (event_date.replace(day=1) +
                      timedelta(days=32)).replace(day=1).strftime("%Y%m")
        year_months.append(next_month)

    return year_months


def download_file(url, save_path):
    try:
        response = requests.get(url, stream=True, timeout=60)
        response.raise_for_status()
        with open(save_path, 'wb') as file:
            for chunk in response.iter_content(chunk_size=8192):
                file.write(chunk)
        print(f"Successfully downloaded {save_path}")
    except requests.RequestException as e:
        print(f"Error downloading {save_path}: {e}")


def process_profile(profile, main_data):
    BASE_URL = "https://psl.noaa.gov/thredds/fileServer/Datasets/NARR/pressure/{}.{}.nc"
    os.makedirs(f'./assets/{profile}', exist_ok=True)

    for _, row in main_data.iterrows():
        year_months = get_relevant_months(row['initialdat'])
        for month in year_months:
            url = BASE_URL.format(profile, month)
            save_path = os.path.join(
                'assets', profile, f"{profile}.{month}.nc")
            download_file(url, save_path)


def main():
    main = gpd.read_file('./full_globfire/full_globfire.shp')
    bottom = main['area_ha'].quantile(0.2)
    main = main[main['area_ha'] > bottom]
    main = pd.concat([main, main.bounds], axis=1)

    profiles_of_interest = ['air', 'hgt',
                            'omega', 'shum', 'tke', 'uwnd', 'vwnd']

    process_profile_partial = partial(process_profile, main_data=main)

    with multiprocessing.Pool() as pool:
        pool.map(process_profile_partial, profiles_of_interest)


if __name__ == "__main__":
    main()
