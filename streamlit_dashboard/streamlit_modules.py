import pandas as pd
from sqlalchemy import create_engine, text
import numpy as np
import seaborn as sns
from sklearn.preprocessing import StandardScaler
import os
import sys
from dotenv import load_dotenv
import geopandas as gpd
import matplotlib.pyplot as plt
import plotly.express as px
import plotly.graph_objs as go
from sf311_preprocess_2 import shape_extract
import json

def pull_pg_table(pg_hostname, pg_userid, pg_pw, db_name, table_name):
    '''
        Description: import table from postgres database. This project uses a postgres database
        running on an AWS EC2 instance to store all final SF 311 data. The user inputs the connection informatoion
        required for the SQLAlchemy create_engine function and the output is a complete dataframe from the specified
        table.
        
        Inputs: 
            pg_hostname: postgres database ip address or dns address
            pg_userid: postgres user id
            pg_pw: postgres password
            db_name: name of database
            table_name: name of table to be pulled
            
        Outputs:
            df: pandas dataframe containing table query output.
    '''
    import pandas as pd
    from sqlalchemy import create_engine, text
    db_url = f'postgresql://{pg_userid}:{pg_pw}@{pg_hostname}:5432/{db_name}'
    engine = create_engine(db_url)
    query = f"SELECT * FROM {table_name}"
    
    with engine.connect() as connection:
        result = connection.execute(query)
        rows = result.fetchall()
    
    # Convert rows to pandas DataFrame
    df = pd.DataFrame(rows, columns=result.keys())
    return df

def add_dow_integers(df, date_str, dow_string):
    #Create dataframe to map day of week to integers
    dow = ['Monday','Tuesday','Wednesday','Thursday','Friday','Saturday','Sunday']
    dow_index = [1,2,3,4,5,6,7]
    dow_df = pd.DataFrame(list(zip(dow_index, dow)), columns=['dow_index',dow_string])
    
    #Merge dow integers with dataframe
    df['month'] = pd.to_datetime(df[date_str]).dt.month
    return pd.merge(df, dow_df, on=dow_string, how='left')

def standardize_data(df):
    #normalize data to compare trends between categories
    scaler = StandardScaler()
    df_norm = scaler.fit_transform(df)
    df_norm = pd.DataFrame(df_norm, columns = df.columns, index = df.index)
    return df_norm

def request_aggregate(df: pd.DataFrame, 
                      agg_fields: list, 
                      agg_type: str,
                      sum_field: str):
    if agg_type == 'sum':
        agg_type = sum
    else:
        agg_type = np.mean
    
    return df.groupby(agg_fields).agg(agg_value = (sum_field, agg_type)).reset_index()

def unpivot_df(df: pd.DataFrame, 
               index: list,
              column_unpiv = str,
              value_column = str):
    
    return df.pivot(index=index, columns=column_unpiv,values=value_column)


def choropleth_map(tract_data, geodf, aggvalue, geoagg, selected_color_scheme):
    #Aggregate values dataframe to appropriate levels
    tract_totals = tract_data.groupby(geoagg).agg(sum_requests = (aggvalue,sum)).reset_index()
    if geoagg == 'tractce':
        merge_cols = ['tractce']
    else:
        merge_cols = ['tractce',geoagg]
    tract_totals = pd.merge(tract_data[merge_cols].drop_duplicates(), tract_totals, on=geoagg, how='left')
    tract_totals['id'] = tract_totals[geoagg]
    tract_totals['tract_int'] = pd.to_numeric(tract_totals['tractce'], errors='coerce', downcast='integer')
    tract_totals = tract_totals.sort_values('tract_int')
    tract_totals = tract_totals.reset_index().drop('index',axis=1).dropna()
    
    #Process geodf and aggregate
    geodf = geodf[geodf[geoagg].isin(tract_totals[geoagg])]
    geodf['geometry'] = geodf['the_geom'].apply(shape_extract)
    geodf_gdf = gpd.GeoDataFrame(geodf, crs='EPSG:4326')
    geodf_gdf['tract_int'] = geodf_gdf['tractce'].astype(int)
    geodf_gdf = geodf_gdf.sort_values('tract_int').reset_index().drop('index',axis=1)
    geodf_gdf['object_id'] = geodf_gdf.reset_index()['index'].astype(str)

    poly_json = json.loads(geodf_gdf.set_index('tractce').to_json())
    
    #Create choropleth
    fig = px.choropleth_mapbox(tract_totals, 
                           geojson=poly_json, 
                           locations='tractce',
                            color="sum_requests",
                            color_continuous_scale = selected_color_scheme,
                            mapbox_style="carto-positron",
                            hover_name = geoagg,
                            center={"lat": 37.784279, "lon": -122.407234},  # Map center on San Francisco
                            zoom=10.3)
    fig.update_layout(
            template='plotly_dark',
            plot_bgcolor='rgba(0, 0, 0, 0)',
            paper_bgcolor='rgba(0, 0, 0, 0)',
            margin=dict(l=0, r=0, t=0, b=0),
            height=350
        )
    #fig.show()
    return poly_json, fig

def moving_average(df, linechart_slice, window_size):
    df_ma = pd.DataFrame()
    window_size = int(window_size)
    for current_value in df[linechart_slice].unique():
        #rolling_avg = pd.DataFrame()
        rolling_avg = pd.DataFrame(df[df[linechart_slice]==current_value]['agg_value'].rolling(window=window_size).mean())
        rolling_avg[linechart_slice] = current_value
        rolling_avg['requested_date'] = df[df[linechart_slice]==current_value]['requested_date']
        df_ma = pd.concat([df_ma, rolling_avg], axis=0, ignore_index=True)
    
    final_df = pd.merge(df, df_ma, on = ['requested_date', linechart_slice], how='left', suffixes=['','_ma'])
    
    return final_df

def linechart_pltly(df, linechart_slice, line_colors, window_size):
    agg_fields = ['requested_date', linechart_slice]
    agg_type = 'sum'
    sum_field = 'daily_requests'
    df_linechart = request_aggregate(df, agg_fields, agg_type, sum_field)
    df_linechart['agg_value'] = df_linechart['agg_value'].astype(int)

    df_linechart_ma = moving_average(df_linechart, linechart_slice, window_size)
    fig = px.line(df_linechart_ma, x = 'requested_date', 
                  y = 'agg_value_ma', 
                  color = linechart_slice,
                  color_discrete_sequence = line_colors)
    return fig

def dow_cat_normalized(df, date_str):
    #Add DOW integers
    date_str = date_str
    dow_string = 'day_of_week'
    df_dow = add_dow_integers(df, date_str, dow_string)

    #Aggregate sum to DOW and Category
    agg_fields = [date_str,'day_of_week','dow_index','service_categories']
    agg_type = 'sum'
    sum_field = 'daily_requests'
    daily_cat = request_aggregate(df_dow, agg_fields, agg_type, sum_field)

    #Aggregate mean to DOW and category
    agg_fields = ['day_of_week','dow_index','service_categories']
    agg_type = 'mean'
    sum_field = 'agg_value'

    dow_cat_avg = request_aggregate(daily_cat, agg_fields, agg_type, sum_field)

    dow_cat_avg_list = dow_cat_avg['service_categories'].drop_duplicates().tolist()

    #Unpivot dataframe
    index = ['day_of_week','dow_index']
    column_unpiv = 'service_categories'
    value_column = 'agg_value'

    dow_cat_avg = unpivot_df(dow_cat_avg, index, column_unpiv, value_column)

    #Standardize data
    dow_cat_avg_norm = standardize_data(dow_cat_avg)

    dow_cat_avg_norm = dow_cat_avg_norm.reset_index()
    dow_cat_avg_norm['map_index'] = dow_cat_avg_norm['dow_index'].astype(str) + ' - ' + dow_cat_avg_norm['day_of_week']
    dow_cat_avg_norm = dow_cat_avg_norm.set_index('map_index')
    dow_cat_avg_norm=dow_cat_avg_norm[[*dow_cat_avg_list]].sort_index()

    return dow_cat_avg_norm

def create_heatmap(df,selected_colorscale, xaxis_name, yaxis_name):
    heatmap_trace = go.Heatmap(z = df.values, x=df.columns, 
           y = df.index,colorscale = selected_colorscale)
    # Define the layout
    layout = go.Layout(
        xaxis=dict(title=xaxis_name),
        yaxis=dict(title=yaxis_name,categoryorder='category descending')
        
    )
    # Create the figure
    fig = go.Figure(data=heatmap_trace, layout=layout)

    return fig