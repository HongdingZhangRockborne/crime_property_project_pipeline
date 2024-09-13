import os
import pandas as pd
import numpy as np

def create_top_5_crime_lst(df):
    top_5_crime_lst = df['Crime type'].value_counts().head().index.tolist()
    return top_5_crime_lst

def create_top_5_crime_df(df):
    top_5_crime_lst = create_top_5_crime_lst(df)
    top_5_crime_df = df[df['Crime type'].isin(top_5_crime_lst)]
    return top_5_crime_df

def create_crime_count_year_month_df(df):
    year_month_crime_count_df = df.groupby(['Date','Crime type']).agg('count')[['Crime ID']].reset_index()
    year_month_crime_count_df = year_month_crime_count_df.sort_values(by='Date', ascending=True)
    return year_month_crime_count_df

def create_top_5_crime_count_year_month_df(df):
    top_5_crime_lst = create_top_5_crime_lst(df)
    year_month_crime_count_df = create_crime_count_year_month_df(df)
    top_5_year_month_crime_count_df = year_month_crime_count_df[year_month_crime_count_df['Crime type'].isin(top_5_crime_lst)]
    return top_5_year_month_crime_count_df

def create_date_location_hotspots_df(df):
    date_location_hotspots_df = df.groupby(['Location', 'Date']).size().unstack().fillna(0)
    return date_location_hotspots_df

def create_top_5_crime_location_lst(df):
    top_crime_location_lst = df.groupby('Location').agg('count')[['Crime ID']].sort_values(by='Crime ID', ascending=False).drop(index='No Info').head().index
    return top_crime_location_lst

def create_top_5_crime_count_location_date_df(df):
    df_top_crime_location_ls = create_top_5_crime_location_lst(df)
    df_crime_hotspots_df_long = create_date_location_hotspots_df(df).reset_index().melt(id_vars='Location', var_name='Date', value_name='Crime Count')
    top_5_crime_count_location_date_df = df_crime_hotspots_df_long[df_crime_hotspots_df_long['Location'].isin(df_top_crime_location_ls)]
    return top_5_crime_count_location_date_df

def create_top_5_crime_count_LSOA_name_df(df):
    top_5_LSOA_name_df = df.groupby('LSOA name').count()[['Crime ID']].sort_values(by='Crime ID', ascending=False).head()
    return top_5_LSOA_name_df

def create_longitute_lantitude_crime_count_df(df):
    # Aggregate the crime count per latitude and longitude
    longitute_lantitude_crime_count_df = df.groupby(['Longitude', 'Latitude']).size().reset_index(name='Crime ID')
    return longitute_lantitude_crime_count_df

def numeric_checked_longitute_lantitude_crime_count_df(df):
    # Ensure numeric conversion
    df['Latitude'] = pd.to_numeric(df['Latitude'], errors='coerce')
    df['Longitude'] = pd.to_numeric(df['Longitude'], errors='coerce')
    
    # Drop any rows with NaNs
    df = df.dropna(subset=['Latitude', 'Longitude', 'Crime ID'])
    
    return df

def create_numberic_checked_longitute_lantitude_crime_count_df(df):
    df = numeric_checked_longitute_lantitude_crime_count_df(create_longitute_lantitude_crime_count_df(df))
    return df

def loop_all_functions(regions_dict):
    report_functions = [create_top_5_crime_count_year_month_df,
                        create_top_5_crime_count_location_date_df, 
                        create_top_5_crime_count_LSOA_name_df, 
                        create_numberic_checked_longitute_lantitude_crime_count_df]
    
    os.chdir('visualisation_dataframe')
    for f in report_functions:
        for key, values in regions_dict.items():
            street_df = f(values)
            street_df.to_csv(f'reporting_{key.split('_')[1]}_{f.__name__.split('_', 1)[1]}')
    os.chdir('../')
    
    return