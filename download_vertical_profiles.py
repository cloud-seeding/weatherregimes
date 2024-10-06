"""
REQUIREMENTS:

requests
pandas
geopandas
shapely

"""


import os
import multiprocessing
from datetime import timedelta
import xarray as xr
import requests
import numpy as np
import pandas as pd
import geopandas as gpd


def adjust_longitude(lon):
    """
    Adjust longitude to the 0 to 360 range required by the dataset if necessary.
    """
    if lon < 0:
        return lon + 360
    return lon


def construct_ncss_url(profile, date):
    """
    Construct the NCSS URL for the given parameters.
    """
    BASE_URL = "https://psl.noaa.gov/thredds/dodsC/Datasets/NARR/pressure/{profile}.{year}{month:02d}.nc"
    url = BASE_URL.format(profile=profile, year=date.year, month=date.month)
    return url


def download_file(url, save_path, lat_min, lon_min, lat_max, lon_max):
    try:
        data = xr.open_dataset(url)
        lon_mask = (data['lon'] >= lon_min) & (data['lon'] <= lon_max)
        lat_mask = (data['lat'] >= lat_min) & (data['lat'] <= lat_max)
        combined_mask = lon_mask & lat_mask

        # Step 2: Find the `x` and `y` indices that match the bounding box
        y_indices, x_indices = np.where(combined_mask)  # Get the indices of True values in the mask

        # Step 3: Get the min and max indices for `x` and `y`
        x_min, x_max = x_indices.min(), x_indices.max()
        y_min, y_max = y_indices.min(), y_indices.max()

        # Step 4: Subset the data using `isel()` with these indices
        subset = data.isel(x=slice(x_min, x_max + 1), y=slice(y_min, y_max + 1))
        subset.save(save_path)
        print(f"Successfully downloaded {save_path}")
    except Exception as e:
        print(f"Error downloading {save_path}: {e}")


def main():
    # Read the data
    main_data = gpd.read_file('./full_globfire/full_globfire.shp')

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
    for _, row in main_data.iterrows():
        event_date = row['initialdat']
        minx, miny, maxx, maxy = row['minx'], row['miny'], row['maxx'], row['maxy']

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
                profile, event_date)

            # Save files with unique names based on event ID and profile
            save_filename = f"{profile}_{row['_uid_']}_{event_date.strftime('%Y%m%d')}.nc"
            save_path = os.path.join(save_dir, save_filename)

            tasks.append((url, save_path, minx, miny, maxx, maxy))

    tasks = list({(task[0], task[1], task[2], task[3], task[4], task[5]) for task in tasks})

    with multiprocessing.Pool() as pool:
        pool.starmap(download_file, tasks)

if __name__ == "__main__":
    main()
