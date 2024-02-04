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