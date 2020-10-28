import geopandas
import pandas as pd
import geopy
from pandarallel import pandarallel 
from pathlib import Path

# init pandarallel for parallilisation
pandarallel.initialize()

file_dir = Path(__file__).parent.parent

df_shape = geopandas.read_file(str(file_dir)+'/OSM_PLZ_072019.shp')

# rename note
df_shape.rename(columns={'note':'ortsname'}, inplace=True)

# create geocoder
geolocator = geopy.Nominatim(user_agent='shp_geocoder', timeout=15)

def get_city_coordinates(x):
    city = x.split(',') # split to prevent fail on complex city names with abbrevations

    gc = geolocator.geocode(city[0], country_codes='de')

    if not gc:
        print('Geolocation could not be found')
        return None
    
    else:
        print(city[0], (gc.latitude, gc.longitude))
        return (gc.latitude, gc.longitude)

# apply parallel for speedup
df_shape['Geocode'] = df_shape['ortsname'].parallel_apply(get_city_coordinates)

# save to csv 
df_shape.to_csv(str(file_dir)+'/shape_plz_geocoded.csv')

