"""
REQUIREMENTS:

requests
pandas
geopandas
shapely

"""


import os
import random
import multiprocessing
from datetime import timedelta
import requests
import pandas as pd
import geopandas as gpd
import cartopy.crs as ccrs
from time import sleep

lambert_crs = ccrs.LambertConformal(
    central_longitude=-107.0,
    central_latitude=50.0,
    standard_parallels=[50.0, 50.0],
    false_easting=5632642.22547,
    false_northing=4612545.65137
)

def adjust_longitude(lon):
    """
    Adjust longitude to the 0 to 360 range required by the dataset if necessary.
    """
    return (lon + 360) % 360

def construct_ncss_url(profile, date, minx, miny, maxx, maxy):
    """
    Construct the NCSS URL for the given parameters.
    """
    BASE_URL = "https://psl.noaa.gov/thredds/ncss/grid/Datasets/NARR/pressure/{profile}.{year}{month:02d}.nc"
    url = BASE_URL.format(profile=profile, year=date.year, month=date.month)

    # NCSS parameters
    params = {
        'var': profile,
        'north': maxy,
        'west': minx,
        'east': maxx,
        'south': miny,
        'horizStride': 1,
        'time': date.strftime('%Y-%m-%dT00:00:00Z'),
        'accept': 'netcdf4-classic'
    }

    # Build the full URL with parameters
    request_url = url + '?' + \
        '&'.join(f"{key}={value}" for key, value in params.items())
    return request_url


def download_file(url, save_path):
    sleep(random.randint(1,5))
    try:
        response = requests.get(url, stream=True, timeout=120)
        response.raise_for_status()
        with open(save_path, 'wb') as file:
            for chunk in response.iter_content(chunk_size=8192):
                file.write(chunk)
        print(f"Successfully downloaded {save_path}")
    except requests.RequestException as e:
        print(f"Error downloading {save_path}: {e}")


def main():
    # Read the data
    main_data = gpd.read_file('./full_globfire/full_globfire.shp').sort_values(by='area_ha',ascending=False)[10:110]

    # Filter data based on 'area_ha' quantile
    bottom = main_data['area_ha'].quantile(0.2)
    main_data = main_data[main_data['area_ha'] > bottom]

    # Ensure 'initialdat' is datetime
    main_data['initialdat'] = pd.to_datetime(
        main_data['initialdat'], errors='coerce')
    main_data = main_data.dropna(subset=['initialdat'])

    # Compute bounds and add them to main_data
    bounds = main_data.geometry.bounds
    main_data = main_data.join(bounds)

    profiles_of_interest = [
        'air',    # Air Temperature
        'hgt',    # Geopotential Height
        'omega',  # Vertical Velocity in Pressure Coordinates
        'shum',   # Specific Humidity
        'tke',    # Turbulent Kinetic Energy
        'uwnd',   # U-component of wind
        'vwnd'    # V-component of wind
    ]

    # Prepare download tasks
    tasks = []
    for i, row in main_data.iterrows():
        event_date = row['initialdat']
        minx, miny, maxx, maxy = adjust_longitude(row['minx']), row['miny'], adjust_longitude(row['maxx']), row['maxy']

        # Ensure coordinates are within valid ranges
        if any(pd.isnull([minx, miny, maxx, maxy])):
            continue  # Skip if coordinates are missing

        # Create a buffer around the fire event
        buffer = 0.5  # degrees
        minx -= buffer
        miny -= buffer
        maxx += buffer
        maxy += buffer

        for profile in profiles_of_interest:
            save_dir = os.path.join('assets', profile)
            os.makedirs(save_dir, exist_ok=True)

            # Construct the NCSS URL
            url = construct_ncss_url(
                profile, event_date, minx, miny, maxx, maxy)

            # Save files with unique names based on event ID and profile
            save_filename = f"{profile}_{row['_uid_']}_{event_date.strftime('%Y%m%d')}.nc"
            save_path = os.path.join(save_dir, save_filename)

            tasks.append((url, save_path))

    tasks = list({(task[0], task[1]) for task in tasks})

    with multiprocessing.Pool() as pool:
        pool.starmap(download_file, tasks)

if __name__ == "__main__":
    main()
