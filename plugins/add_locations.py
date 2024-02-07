


zoning_endpoints = '3i4a-hu95'
sf_find_neighborhoods = 'pty2-tcw4'
sf_analysis_neighborhoods = 'p5b7-5n3h'
sf_census_neighborhoods = 'sevw-6tgi'
police_district = 'd4vc-q76h'

def shape_extract(x):
    try:
        # Code that may raise an exception
        return shape(x)
    except Exception as e:
        return None

def spatial_join(df, df_spatial_txt, parcels_df):
    #Spatially join fire incidents points with parcel polygons to add parcel data
    geodf = gpd.GeoDataFrame(df,
        geometry=gpd.points_from_xy(df.long, df.lat,
                                    crs='EPSG:4326'))
    parcels_df['geometry'] = parcels_df[df_spatial_txt].apply(shape_extract)
    parcels_geodf = gpd.GeoDataFrame(parcels_df, crs='EPSG:4326')
    joined_geo_df = gpd.sjoin(geodf, parcels_geodf, how="left", predicate='within')
    return df, joined_geo_df
