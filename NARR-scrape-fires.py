import requests
import pandas as pd
import geopandas as gpd
from datetime import datetime,timedelta
import multiprocessing

main = gpd.read_file('../data/full_globfire.shp')

#Drop bottom 20% area fires
bottom = main['area_ha'].quantile(0.2)
main = main[main['area_ha'] > bottom]

#Add bounding boxes for scrape
main = pd.concat([main,main.bounds], axis=1)

for row in main.iterrows():
    start = datetime(row['initialdate']) - timedelta(days=2)
    yearmonth = start.strftime("%Y%m")
    start = start.strftime("%Y-%m-%dT%H:%M:%S")
    end = datetime(row['initialdate']).strftime("%Y-%m-%dT%H:%M:%S")
    N,W,E,S = row['ymax'],row['xmin'],row['xmax'],row['ymin']

    #Need to convert to Netcdfsubset lat-lon system. involves subtracting longitude by 180 if its > 360?? idk something like this.. 

    requests.get(f"https://psl.noaa.gov/thredds/ncss/grid/Datasets/NARR/pressure/air.{yearmonth}.nc?north={N}&west={W}&east={E}&south={S}&horizStride=1&time_start={start}Z&time_end={end}Z&&&accept=netcdf4-classic")