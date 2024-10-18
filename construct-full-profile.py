'''
This script is for taking the individual pressure-level variables - air, shum, etc. - for a given fire event, 
indexed by globfire's _uid_, and combining them into one file in the folder 'all'. 
'''

import os
import numpy as np
import xarray as xr
import pandas as pd
import geopandas as gpd
import cartopy.crs as ccrs
import matplotlib.pyplot as plt
from datetime import datetime
from alive_progress import alive_bar

main = gpd.read_file('full_globfire/full_globfire.shp').sort_values(by='area_ha',ascending=False)

def vertical_stack(id):

    date = main[main['_uid_']==id]['initialdat'].values[0].astype('M8[D]').astype(str).replace('-', '')
    id = "{:.1f}".format(id)

    file_path = f'assets/all/all_{id}_{date}.nc'

    # if os.path.exists(file_path):
    #     return
    
    air = xr.open_dataset(f"assets/air/air_{id}_{date}.nc")
    uwnd = xr.open_dataset(f"assets/uwnd/uwnd_{id}_{date}.nc")
    vwnd = xr.open_dataset(f"assets/vwnd/vwnd_{id}_{date}.nc")
    shum = xr.open_dataset(f"assets/shum/shum_{id}_{date}.nc")
    omega = xr.open_dataset(f"assets/omega/omega_{id}_{date}.nc")
    hgt = xr.open_dataset(f"assets/hgt/hgt_{id}_{date}.nc")
    full = xr.merge([air,shum,omega,hgt,uwnd,vwnd]).drop_vars(['Lambert_Conformal'])

    for var_name in full.data_vars:
        var = full[var_name]
        var.encoding['_FillValue'] = var.encoding['missing_value']

    full.to_netcdf(file_path)

with alive_bar(len(main['_uid_'])) as bar:
    for id in main['_uid_']:
        try:
            vertical_stack(id)
        except Exception as e:
            pass
        finally:
            bar()