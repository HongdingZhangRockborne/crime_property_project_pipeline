import os
import pandas as pd
import numpy as np

def create_top_5_crime_lst(df):
    """
    Creates a list of the top 5 most frequent crime types.
    
    Args:
        df (pd.DataFrame): DataFrame containing crime data with a 'Crime type' column.
        
    Returns:
        list: A list of the top 5 crime types.
    """
    top_5_crime_lst = df['Crime type'].value_counts().head().index.tolist()
    return top_5_crime_lst

def create_top_5_crime_df(df):
    """
    Filters the DataFrame to include only the top 5 crime types.
    
    Args:
        df (pd.DataFrame): DataFrame containing crime data.
        
    Returns:
        pd.DataFrame: A DataFrame filtered to include only the top 5 crime types.
    """
    top_5_crime_lst = create_top_5_crime_lst(df)
    top_5_crime_df = df[df['Crime type'].isin(top_5_crime_lst)]
    return top_5_crime_df

def create_crime_count_year_month_df(df):
    """
    Creates a DataFrame counting crimes per year and month, grouped by crime type.
    
    Args:
        df (pd.DataFrame): DataFrame containing crime data.
        
    Returns:
        pd.DataFrame: A DataFrame with crime counts grouped by year, month, and crime type.
    """
    year_month_crime_count_df = df.groupby(['Date', 'Crime type']).agg('count')[['Crime ID']].reset_index()
    year_month_crime_count_df = year_month_crime_count_df.sort_values(by='Date', ascending=True)
    return year_month_crime_count_df

def create_top_5_crime_count_year_month_df(df):
    """
    Creates a DataFrame of the top 5 crimes, counting occurrences per year and month.
    
    Args:
        df (pd.DataFrame): DataFrame containing crime data.
        
    Returns:
        pd.DataFrame: A DataFrame with crime counts for the top 5 crime types grouped by year and month.
    """
    top_5_crime_lst = create_top_5_crime_lst(df)
    year_month_crime_count_df = create_crime_count_year_month_df(df)
    top_5_year_month_crime_count_df = year_month_crime_count_df[year_month_crime_count_df['Crime type'].isin(top_5_crime_lst)]
    return top_5_year_month_crime_count_df

def create_date_location_hotspots_df(df):
    """
    Creates a pivot table of crime counts by location and date.
    
    Args:
        df (pd.DataFrame): DataFrame containing crime data.
        
    Returns:
        pd.DataFrame: A pivot table DataFrame with locations as rows and dates as columns.
    """
    date_location_hotspots_df = df.groupby(['Location', 'Date']).size().unstack().fillna(0)
    return date_location_hotspots_df

def create_top_5_crime_location_lst(df):
    """
    Creates a list of the top 5 locations with the most crimes, excluding 'No Info'.
    
    Args:
        df (pd.DataFrame): DataFrame containing crime data.
        
    Returns:
        pd.Index: An index object containing the top 5 crime locations.
    """
    top_crime_location_lst = df.groupby('Location').agg('count')[['Crime ID']].sort_values(by='Crime ID', ascending=False).drop(index='No Info').head().index
    return top_crime_location_lst

def create_top_5_crime_count_location_date_df(df):
    """
    Creates a DataFrame of crime counts by date for the top 5 crime locations.
    
    Args:
        df (pd.DataFrame): DataFrame containing crime data.
        
    Returns:
        pd.DataFrame: A DataFrame with crime counts by date for the top 5 locations.
    """
    df_top_crime_location_ls = create_top_5_crime_location_lst(df)
    df_crime_hotspots_df_long = create_date_location_hotspots_df(df).reset_index().melt(id_vars='Location', var_name='Date', value_name='Crime Count')
    top_5_crime_count_location_date_df = df_crime_hotspots_df_long[df_crime_hotspots_df_long['Location'].isin(df_top_crime_location_ls)]
    return top_5_crime_count_location_date_df

def create_top_5_crime_count_LSOA_name_df(df):
    """
    Creates a DataFrame of the top 5 LSOA names by crime count.
    
    Args:
        df (pd.DataFrame): DataFrame containing crime data.
        
    Returns:
        pd.DataFrame: A DataFrame of the top 5 LSOA names by crime count.
    """
    top_5_LSOA_name_df = df.groupby('LSOA name').count()[['Crime ID']].sort_values(by='Crime ID', ascending=False).head()
    return top_5_LSOA_name_df

def create_longitute_lantitude_crime_count_df(df):
    """
    Aggregates crime counts by latitude and longitude.
    
    Args:
        df (pd.DataFrame): DataFrame containing crime data with 'Longitude' and 'Latitude' columns.
        
    Returns:
        pd.DataFrame: A DataFrame with crime counts grouped by latitude and longitude.
    """
    longitute_lantitude_crime_count_df = df.groupby(['Longitude', 'Latitude']).size().reset_index(name='Crime ID')
    return longitute_lantitude_crime_count_df

def numeric_checked_longitute_lantitude_crime_count_df(df):
    """
    Ensures latitude and longitude are numeric and drops rows with missing values.
    
    Args:
        df (pd.DataFrame): DataFrame containing crime data with 'Longitude' and 'Latitude' columns.
        
    Returns:
        pd.DataFrame: A DataFrame with numeric latitude and longitude values, without NaNs.
    """
    df['Latitude'] = pd.to_numeric(df['Latitude'], errors='coerce')
    df['Longitude'] = pd.to_numeric(df['Longitude'], errors='coerce')
    df = df.dropna(subset=['Latitude', 'Longitude', 'Crime ID'])
    return df

def create_numberic_checked_longitute_lantitude_crime_count_df(df):
    """
    Combines latitude and longitude crime counts with numeric checks.
    
    Args:
        df (pd.DataFrame): DataFrame containing crime data.
        
    Returns:
        pd.DataFrame: A DataFrame with numeric latitude and longitude values and crime counts.
    """
    df = numeric_checked_longitute_lantitude_crime_count_df(create_longitute_lantitude_crime_count_df(df))
    return df

def loop_all_functions(regions_dict):
    """
    Loops through the provided functions, applying them to each region's DataFrame.
    Saves the output to CSV files in the 'visualisation_dataframe' directory.
    
    Args:
        regions_dict (dict): Dictionary where keys are region names and values are DataFrames with crime data.
        
    Returns:
        None
    """
    report_functions = [create_top_5_crime_count_year_month_df,
                        create_top_5_crime_count_location_date_df, 
                        create_top_5_crime_count_LSOA_name_df, 
                        create_numberic_checked_longitute_lantitude_crime_count_df]
    
    os.chdir('visualisation_dataframe')
    for f in report_functions:
        for key, values in regions_dict.items():
            street_df = f(values)
            street_df.to_csv(f'reporting_{key.split("_")[1]}_{f.__name__.split("_", 1)[1]}')
    os.chdir('../')
    
    return
