import streamlit as st
import altair as alt
import os
import sys
import geopandas as gpd
import json
import cmasher as cmr
from streamlit_plotly_events import plotly_events
import pandas as pd
# Get the current working directory
cwd = os.getcwd()
# Navigate up one level in the directory hierarchy
parent_dir = os.path.abspath(os.path.join(cwd, '..'))
working_dir = os.getcwd()
relative_path = os.path.join(parent_dir+'/plugins/')
# Add plugins folder for access to functions
sys.path.append(relative_path)
sys.path.append(parent_dir)
from streamlit_modules import pull_pg_table, add_dow_integers, standardize_data, request_aggregate, unpivot_df, choropleth_map, linechart_pltly, dow_cat_normalized, create_heatmap
from sf311_extract import pull_opensf_data
from sf311_preprocess_2 import shape_extract
from dotenv import load_dotenv

def import_env_var():


    #Credentials loaded from .env file, new users of this notebook must create their own credentials and .env file
    dotenv_path = os.path.join(parent_dir+'/dags/.env')

    # Read the contents of the .env file and parse environment variables
    with open(dotenv_path, 'r') as file:
        for line in file:
            if line.strip() and not line.startswith('#'):
                key, value = line.strip().split('=', 1)
                os.environ[key] = value

    #Pull specific environmental variables from dotenv path
    pg_hostname = os.getenv('POSTGRES_HOST')
    pg_userid = os.getenv('AIRFLOW_USER')
    pg_pw = os.getenv('AIRFLOW_PW')

    #Env Variables for OpenData SF
    app_token = os.getenv('APP_TOKEN')
    api_key_id = os.getenv('API_KEY_ID')
    api_secret_key = os.getenv('API_SECRET_KEY')

    #Additional variables not in dotenv
    db_name = 'sf311_data'
    daily_table_name = 'daily_total'
    daily_geo_table_name = 'daily_by_geo'
    sfdata_site = 'data.sfgov.org'
    geo_endpoint = 'sevw-6tgi' # neighborhoods with census tract geo endpoint
    pull_type = 'all'

    return pg_hostname, pg_userid, pg_pw, app_token, api_key_id, api_secret_key, db_name, \
            daily_table_name, daily_geo_table_name, sfdata_site, geo_endpoint, pull_type



def import_data():
#set up streamlit connection
    pg_hostname, pg_userid, pg_pw, app_token, api_key_id, api_secret_key, db_name,\
            daily_table_name, daily_geo_table_name, sfdata_site, geo_endpoint, pull_type = import_env_var()
    conn = st.connection(
        db_name,
        type = "sql",
        url=f'postgresql://{pg_userid}:{pg_pw}@{pg_hostname}:5432/{db_name}'
    )

    #import data
    df_daily = conn.query(f"select * from {daily_table_name}")
    tract_df = conn.query(f"select * from {daily_geo_table_name}")
    tract_map = pull_opensf_data(sfdata_site, 
                        geo_endpoint, 
                        app_token, 
                        api_key_id, 
                        api_secret_key,
                        pull_type)
    
    return df_daily, tract_df, tract_map

def initialize_state():
    for q in ['selected_locations','selected_tracts', 'selected_neighborhoods']:
        if f"{q}_query" not in st.session_state:
            st.session_state[f"{q}_query"] = set()

    if "counter" not in st.session_state:
        st.session_state.counter = 0


def load_sidebar_selectors(df_daily):
    #Format select boxes and pull current user-defined values
    with st.sidebar:
        st.title('San Francisco 311 Complaint Dashboard')
        
        category_list = ['all']
        category_list.extend(list(df_daily.service_categories.unique())[::-1])
        selected_category = st.selectbox('Select a category', category_list, index=category_list.index('all'))
        # df_selected_year = df_reshaped[df_reshaped.year == selected_year]
        # df_selected_year_sorted = df_selected_year.sort_values(by="population", ascending=False)

        color_theme_list = ['blues', 'cividis', 'greens', 'inferno', 'magma', 'plasma', 'reds', 'rainbow', 'turbo', 'viridis']
        selected_color_theme = st.selectbox('Select a color theme for Dashboard', color_theme_list, index=color_theme_list.index('inferno'))

        spatial_aggregation_list = ['Census Tract', 'Neighborhood']
        selected_spatial_agg = st.selectbox('Select a spatial aggregation for map', spatial_aggregation_list)
        
        linechart_list = ['agency_categories','neighborhoods_analysis_boundaries','service_categories']
        linechart_slice = st.selectbox('Select an aggregation for linechart', linechart_list)

        moving_average_list = ['Daily', '7-Day', '30-Day']
        selected_moving_average = st.selectbox('Select a moving average for the linechart', moving_average_list)

    if selected_spatial_agg =='Census Tract':
        spatial_agg = 'tractce'
    else:
        spatial_agg = 'neighborhoods_analysis_boundaries'
    
    return selected_category, selected_color_theme, selected_spatial_agg, linechart_slice,\
          selected_moving_average, spatial_agg


def filter_agg_data(df_daily, tract_df, tract_map, selected_category, spatial_agg):
    #Filter dataframes based on user-defined values for select boxes
    if selected_category == 'all':
        pass
    else:
        df_daily = df_daily[df_daily['service_categories']==selected_category]
        tract_df = tract_df[tract_df['service_categories']==selected_category]


    #Create choropleth map using tract data and GeoJSON
    #Aggregate tract daily data by census tract this may be unnecessary
    agg_fields = 'tractce'
    agg_type = 'sum'
    sum_field = 'daily_requests'
    tract_join_totals = request_aggregate(tract_df, agg_fields, agg_type, sum_field)
    spatial_agg_totals = request_aggregate(tract_df, spatial_agg, agg_type, sum_field)

    #Rename tractce column to match what will come out of GeoPandas dataframe this may be unnecessary
    tract_join_totals['id'] = tract_join_totals['tractce']

    #Only include overlapping tracts this may be unnecessary
    tract_map = tract_map[tract_map['tractce'].isin(tract_join_totals['tractce'])]

    #Convert to geometry type
    tract_map['geometry'] = tract_map['the_geom'].apply(shape_extract)

    return spatial_agg_totals, tract_map


def build_ui(tract_df, tract_map, sum_field, spatial_agg, selected_color_theme,
             linechart_slice, selected_moving_average, spatial_agg_totals, selected_spatial_agg,
             df_daily):
    #Call choropleth function
    col = st.columns((6, 4), gap='medium')
    with col[0]:
        st.markdown('#### Total Complaints by Tract')
        tract_data = tract_df
        geodf = tract_map
        poly_json, choropleth = choropleth_map(tract_data, geodf, sum_field, spatial_agg, selected_color_theme)
        # st.plotly_chart(choropleth, use_container_width=True)
        clicked_locations = plotly_events(choropleth, 
                                        select_event=True,
                                        click_event= True,
                                        key = f"selected_locations_{st.session_state.counter}"
                                        )
        
        #st.write(clicked_locations)
        current_query = {}
        current_query['selected_locations_query'] = {f"{el['pointIndex']}" for el in clicked_locations}

        st.markdown('#### Daily Complaints Over Time')

        line_colors = cmr.take_cmap_colors(selected_color_theme, N = len(tract_df[linechart_slice].drop_duplicates()), return_fmt='hex')
        

        if selected_moving_average == 'Daily':
            window_size = 1
        elif selected_moving_average == '7-Day':
            window_size = 7
        else: 
            window_size = 30

        if str(st.session_state['selected_tracts_query']) != 'set()':
            if spatial_agg == 'tractce':
                selected_tracts = list(map(str,st.session_state['selected_tracts_query']))
                line_input_df = tract_df[tract_df[spatial_agg].isin(selected_tracts)]
            else:
                selected_neighborhoods = list(map(str,st.session_state['selected_neighborhoods_query']))
                line_input_df = tract_df[tract_df[spatial_agg].isin(selected_neighborhoods)]
        else:
            line_input_df = tract_df

        linechart_plt = linechart_pltly(line_input_df, linechart_slice, line_colors, window_size)
        st.plotly_chart(linechart_plt, use_container_width=True)

    spatial_totals_sorted = spatial_agg_totals.sort_values('agg_value', ascending=False)
    spatial_totals_sorted = spatial_totals_sorted.rename(columns={'agg_value':'Sum Requests'})


    with col[1]:
        st.markdown(f'#### Top {selected_spatial_agg}s')

        st.dataframe(spatial_totals_sorted,
                    column_order=(spatial_agg, "Sum Requests"),
                    hide_index=True,
                    width=None,
                    column_config={
                        spatial_agg: st.column_config.TextColumn(
                            selected_spatial_agg,
                        ),
                        "Sum Requests": st.column_config.ProgressColumn(
                            "Sum Requests",
                            format="%f",
                            min_value=0,
                            max_value=max(spatial_totals_sorted['Sum Requests']),
                        )}
                    )
        
        date_str = 'requested_date'
        xaxis_name = 'Service Category'
        yaxis_name = 'Day of Week'
        df_daily_norm = dow_cat_normalized(df_daily, date_str)
        df_daily_heatmap = create_heatmap(df_daily_norm,
                                        selected_color_theme, 
                                        xaxis_name, 
                                        yaxis_name)
        st.markdown('#### Daily Normalized Heatmap by Category')
        st.plotly_chart(df_daily_heatmap, use_container_width=True)

    return clicked_locations, current_query, poly_json

def update_state(current_query):
    """Stores input dict of filters into Streamlit Session State.

    If one of the input filters is different from previous value in Session State, 
    rerun Streamlit to activate the filtering and plot updating with the new info in State.
    """
    rerun = False
    for q in ['selected_locations','selected_tracts', 'selected_neighborhoods']:
        if current_query[f"{q}_query"] - st.session_state[f"{q}_query"]:
            st.session_state[f"{q}_query"] = current_query[f"{q}_query"]
            rerun = True

    if rerun:
        st.experimental_rerun()

def extract_json_info(poly_json, tract_df, index):
    # index_list = list(map(int, index))
    # index_list = [i for i in index_list]
    index_list = list(map(str, index))
    json_df = pd.DataFrame(poly_json)
    json_df = pd.json_normalize(json_df['features'])
    unique_tracts = tract_df[['tractce','neighborhoods_analysis_boundaries']].drop_duplicates()
    json_df = pd.merge(json_df, unique_tracts, left_on = ['id'], right_on = ['tractce'], how = 'left')
    selected_tract = json_df[json_df['properties.object_id'].isin(index_list)]['tractce'].drop_duplicates().to_list()
    selected_neighborhood = json_df[json_df['properties.object_id'].isin(index_list)]['neighborhoods_analysis_boundaries'].drop_duplicates().to_list()
    return selected_tract, selected_neighborhood

def main():
    st.set_page_config(
        page_title="San Francisco 311 Complaint Data Dashboard",
        layout="wide",
        initial_sidebar_state="expanded"
        )

    alt.themes.enable("dark")

    try:
        st.write(str(st.session_state["selected_neighborhoods_query"]))
    except:
        pass

    df_daily, tract_df, tract_map = import_data()

    selected_category, selected_color_theme, selected_spatial_agg, linechart_slice,\
          selected_moving_average, spatial_agg = load_sidebar_selectors(df_daily)
    
    spatial_agg_totals, tract_map = filter_agg_data(df_daily, tract_df, tract_map, selected_category, spatial_agg)
    
    sum_field = 'daily_requests'

    clicked_locations, current_query, poly_json = build_ui(tract_df, tract_map, sum_field, spatial_agg, selected_color_theme,
             linechart_slice, selected_moving_average, spatial_agg_totals, selected_spatial_agg,
             df_daily)
    #st.write(poly_json['features'][230]['properties']['tract_int'])
    selected_tract, selected_neighborhood = extract_json_info(poly_json, tract_df, current_query['selected_locations_query'])
    current_query[f"selected_tracts_query"] = {tract for tract in selected_tract}
    current_query[f"selected_neighborhoods_query"] = {neighborhood for neighborhood in selected_neighborhood}
    update_state(current_query)

    
    
if __name__ == "__main__":
    initialize_state()
    main()
