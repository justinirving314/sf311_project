#Use Nominatim to add coordiantes to addresses that were missing them originally from SF 311
#Some address cannot get coordinates using this method, such as Intersections.
def address_to_coordinates(df):
    app_name = 'sfpolice_data_app'
    from geopy.geocoders import Nominatim
    from geopy.exc import GeocoderTimedOut   
    import pandas as pd
    
    geolocator = Nominatim(user_agent=app_name)  # Replace "your_app_name" with a unique name for your application
    new_lat = []
    new_long = []
    new_service = []
    
    for index, row in df.iterrows():

        lat = row['lat']
        address = row['address']
        service_id = row['service_request_id']
        
        
        if abs(float(lat)) < 1:

            if 'intersection' in str.lower(address):
                continue #Skip processing for addresses containing 'Intersection' as Nominatim cannot handle them
            elif 'specific address' in str.lower(address):
                continue
            else: 
                try:
                    location = geolocator.geocode(address)

                    if location:
                        latitude, longitude = location.latitude, location.longitude
                        #print('Address: '+str(address)+'latitude: '+str(latitude)+' longitude: '+str(longitude))
                        new_lat.append(latitude)
                        new_long.append(longitude)
                        new_service.append(service_id)
                    else:
                        continue

                except (GeocoderTimedOut, Exception) as e:
                    print(f"Error geocoding address '{address}': {e}")

        else:
            continue
        
    results_df = pd.DataFrame({'service_request_id': new_service, 'lat': new_lat, 'long': new_long})
    merged_df = pd.merge(df, results_df, on='service_request_id', how='left', suffixes=('_original', '_new'))
    merged_df['lat'] = merged_df['lat_new'].combine_first(merged_df['lat_original'])
    merged_df['long'] = merged_df['long_new'].combine_first(merged_df['long_original'])
    # Drop unnecessary columns
    merged_df = merged_df.drop(['lat_original', 'lat_new','long_original', 'long_new'], axis=1)
    
    return merged_df


def shape_extract(x):
    from shapely.geometry import shape
    from shapely.geometry import Point
    try:
        # Code that may raise an exception
        return shape(x)
    except Exception as e:
        return None


def add_tracts(df, 
                df_spatial_txt, 
                site,
                geo_endpoint,
                app_token, 
                api_key_id, 
                api_secret_key):
    #Spatially join fire incidents points with parcel polygons to add parcel data
    from sf311_extract import pull_opensf_data
    from sf311_preprocess_2 import shape_extract
    import geopandas as gpd
    import pandas as pd
    from shapely.geometry import shape
    from shapely.geometry import Point
    import numpy as np
    import pickle
    import json
    from airflow.models import TaskInstance

    geodf = gpd.GeoDataFrame(df,
                            geometry=gpd.points_from_xy(df.long, 
                                                            df.lat,
                                                            crs='EPSG:4326'))
        
    pulltype = 'all'
    print('App_token: '+str(app_token))
    parcels_df = pull_opensf_data(site, 
                     geo_endpoint, 
                     app_token, 
                     api_key_id, 
                     api_secret_key,
                     pulltype)

    parcels_df['geometry'] = parcels_df[df_spatial_txt].apply(shape_extract)
    parcels_geodf = gpd.GeoDataFrame(parcels_df, crs='EPSG:4326')
    joined_geo_df = gpd.sjoin(geodf, parcels_geodf, how="left", predicate='within')
    # Assuming 'df' is your DataFrame
    df_json = pd.DataFrame(joined_geo_df.drop('geometry',axis=1)\
                           [['service_request_id','tractce','name','neighborhoods_analysis_boundaries']])

    df_final = pd.merge(df,df_json,on='service_request_id',how='left')

    return df_final