import streamlit as st
import altair as alt
import os
import sys
import geopandas as gpd
import json
import cmasher as cmr
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


st.set_page_config(
    page_title="San Francisco 311 Complaint Data Dashboard",
    layout="wide",
    initial_sidebar_state="expanded"
    )

alt.themes.enable("dark")

#set up streamlit connection
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

#Format select boxes and pull current user-defined values
with st.sidebar:
    st.title('San Francisco 311 Complaint Dashboard')
    
    category_list = ['all']
    category_list.extend(list(df_daily.service_categories.unique())[::-1])
    selected_category = st.selectbox('Select a category', category_list, index=category_list.index('all'))
    # df_selected_year = df_reshaped[df_reshaped.year == selected_year]
    # df_selected_year_sorted = df_selected_year.sort_values(by="population", ascending=False)

    color_theme_list = ['blues', 'cividis', 'greens', 'inferno', 'magma', 'plasma', 'reds', 'rainbow', 'turbo', 'viridis']
    selected_color_theme = st.selectbox('Select a color theme for Dashboard', color_theme_list)

    spatial_aggregation_list = ['Census Tract', 'Neighborhood']
    selected_spatial_agg = st.selectbox('Select a spatial aggregation for map', spatial_aggregation_list)
    
    linechart_list = ['agency_categories','neighborhoods_analysis_boundaries','service_categories']
    linechart_slice = st.selectbox('Select an aggregation for linechart', linechart_list)

if selected_spatial_agg =='Census Tract':
    spatial_agg = 'tractce'
else:
    spatial_agg = 'neighborhoods_analysis_boundaries'

#Filter dataframes based on user-defined values for select boxes
if selected_category == 'all':
    pass
else:
    df_daily = df_daily[df_daily['service_categories']==selected_category]
    tract_df = tract_df[tract_df['service_categories']==selected_category]


col = st.columns((6, 4), gap='medium')
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

#Convert to geopandas dataframe
tract_map_gdf = gpd.GeoDataFrame(tract_map, crs='EPSG:4326')

#Convert to JSON
poly_json = json.loads(tract_map_gdf[['geometry','tractce']].set_index('tractce').to_json())

#Call choropleth function
with col[0]:
    st.markdown('#### Total Complaints by Tract')
    tract_data = tract_df
    geodf = tract_map
    choropleth = choropleth_map(tract_data, geodf, sum_field, spatial_agg, selected_color_theme)
    st.plotly_chart(choropleth, use_container_width=True)

    st.markdown('#### Daily Complaints Over Time')

    line_colors = cmr.take_cmap_colors(selected_color_theme, N = len(tract_df[linechart_slice].drop_duplicates()), return_fmt='hex')
    linechart_plt = linechart_pltly(tract_df, linechart_slice, line_colors)
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