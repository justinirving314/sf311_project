import pandas as pd
from sodapy import Socrata
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut   

def shape_extract(self, x):
    try:
        # Code that may raise an exception
        return shape(x)
    except Exception as e:
        return None

class sfdata_el_process:
    
    def __init__(self, 
                        sf_app_token,
                        sf_api_key_id,
                        sf_api_secret_key,
                        nom_app_name,
                        aws_key_id,
                        aws_secret_key):
        

        
        self.sf_app_token = sf_app_token
        self.sf_api_key_id = sf_api_key_id
        self.sf_api_secret_key = sf_api_secret_key
        self.nom_app_name = nom_app_name
        self.aws_key_id = aws_key_id
        self.aws_secret_key = aws_secret_key


    def pull_opensf_data(self, 
                     site, 
                     endpoint, 
                     pulltype,
                     filters = None):
        """
            Arguments: 
                site: sf data website
                app_token: app_token created during set-up. 
                api_key_id: api key id created during set-up
                api_secret_key: secret api key created during set-up

            Results:
                all_records: a pandas dataframe of all records returned by the API pulls

        """
        self.opensf_site = site
        app_token = self.sf_app_token
        api_key_id = self.sf_api_key_id
        api_secret_key = self.sf_api_secret_key

        #set limit and offset and create empty dataframe
        all_records = pd.DataFrame()
        limit = 40000
        offset = 0

        # Example authenticated client (needed for non-public datasets):
        client = Socrata(site,
                        app_token,
                        username=api_key_id,
                        password=api_secret_key)

        while True:
            results = client.get(endpoint, limit = limit, offset = offset, where=filters)
            
            #If there are no results leave the loop
            if not results:
                break

            #If results exist convert to dataframe and concat with existing running total dataframe
            results_df = pd.DataFrame.from_records(results)
            all_records = pd.concat([all_records, results_df], ignore_index=True)

            #Added an input in case we just want one sample of data instead of whole set
            if pulltype == 'test':
                break

            #Add the limit amount to the offset to keep cycling through the data
            offset += limit


        # Convert to pandas DataFrame
        
        self.raw_results = all_records
        return all_records
    
    #Modify data types of certain columns prior to uploading
    def raw_311_preprocess(self):
        raw_311_results = self.raw_results #pull from object
        raw_311_results['service_request_id'] = raw_311_results['service_request_id'].astype(int)
        raw_311_results['requested_datetime'] = pd.to_datetime(raw_311_results['requested_datetime'])
        raw_311_results['updated_datetime'] = pd.to_datetime(raw_311_results['updated_datetime'])
        raw_311_results['lat'] = raw_311_results['lat'].astype(float)
        raw_311_results['long'] = raw_311_results['long'].astype(float)
        
        # Specify the character you want to filter columns by
        character_to_filter = '@'

        # Drop columns containing the specified character
        filtered_columns = raw_311_results.filter(like=character_to_filter, axis=1)
        int_311_results = raw_311_results.drop(columns=filtered_columns.columns)

        self.int_results = int_311_results
        
        return int_311_results
    
    #Use Nominatim to add coordiantes to addresses that were missing them originally from SF 311
    #Some address cannot get coordinates using this method, such as Intersections.
    def address_to_coordinates(self):
        app_name = self.nom_app_name
        df = self.int_results

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
        self.int_311_results = merged_df
        return merged_df
    


    def add_tracts(self, 
                     df_spatial_txt, 
                     geo_endpoint):
        #Spatially join fire incidents points with parcel polygons to add parcel data
        df = self.int_311_results
        geodf = gpd.GeoDataFrame(df,
                                geometry=gpd.points_from_xy(df.long, 
                                                            df.lat,
                                                            crs='EPSG:4326'))
        
        pulltype = 'all'
        parcels_df = self.pull_opensf_data(self.opensf_site, 
                                          geo_endpoint, 
                                          pulltype)

        parcels_df['geometry'] = parcels_df[df_spatial_txt].apply(shape_extract)
        parcels_geodf = gpd.GeoDataFrame(parcels_df, crs='EPSG:4326')
        joined_geo_df = gpd.sjoin(geodf, parcels_geodf, how="left", predicate='within')
        return joined_geo_df