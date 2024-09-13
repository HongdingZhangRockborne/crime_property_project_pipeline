import matplotlib.pyplot as plt
import seaborn as sns
import os
import pandas as pd
import numpy as np
import folium
from folium.plugins import HeatMap
%matplotlib inline

def group_by_date_month(df):
    """
    Groups the DataFrame by the 'Date month' column and counts the number of crimes.

    Args:
        df (pd.DataFrame): DataFrame containing crime data, including 'Date month' and 'Crime ID' columns.

    Returns:
        pd.DataFrame: A DataFrame with the count of crimes per month.
    """
    groupby_df = df.groupby('Date month').agg('count')[['Crime ID']]
    return groupby_df


def create_top_5_crime_lst(df):
    """
    Identifies the top 5 most common crime types in the DataFrame.

    Args:
        df (pd.DataFrame): DataFrame containing crime data, including the 'Crime type' column.

    Returns:
        list: A list of the top 5 most frequent crime types.
    """
    top_5_crime_lst = df['Crime type'].value_counts().head().index.tolist()
    return top_5_crime_lst


def create_top_5_crime_df(df):
    """
    Filters the DataFrame to include only the top 5 most common crime types.

    Args:
        df (pd.DataFrame): DataFrame containing crime data.

    Returns:
        pd.DataFrame: A DataFrame filtered to include only the top 5 most frequent crime types.
    """
    top_5_crime_lst = create_top_5_crime_lst(df)
    top_5_crime_df = df[df['Crime type'].isin(top_5_crime_lst)]
    return top_5_crime_df


def create_month_crime_count_df(df):
    """
    Creates a DataFrame showing the monthly crime count for each crime type.

    Args:
        df (pd.DataFrame): DataFrame containing crime data, including 'Date month' and 'Crime type' columns.

    Returns:
        pd.DataFrame: A DataFrame with the count of crimes per month, sorted by the crime count in descending order.
    """
    month_crime_count_df = df.groupby(['Date month', 'Crime type']).agg('count')[['Crime ID']].reset_index().sort_values(by='Crime ID', ascending=False)
    return month_crime_count_df


def create_top_5_month_crime_count_df(df):
    """
    Filters the monthly crime count DataFrame to include only the top 5 most common crime types.

    Args:
        df (pd.DataFrame): DataFrame containing crime data.

    Returns:
        pd.DataFrame: A DataFrame showing the monthly crime count for the top 5 crime types.
    """
    top_5_crime_lst = create_top_5_crime_lst(df)
    month_crime_count_df = create_month_crime_count_df(df)
    top_5_month_crime_count_df = month_crime_count_df[month_crime_count_df['Crime type'].isin(top_5_crime_lst)]
    return top_5_month_crime_count_df


def plot_date_month_crime_id_count(df):
    """
    Plots the number of crimes by month.

    Args:
        df (pd.DataFrame): DataFrame containing crime data, including 'Date month' and 'Crime ID' columns.

    Returns:
        matplotlib.axes._subplots.AxesSubplot: A bar plot showing the number of crimes per month.
    """
    ax = sns.barplot(data=df.groupby('Date month').agg('count')[['Crime ID']], x=df.groupby('Date month').agg('count')[['Crime ID']].index, y='Crime ID')
    ax.set(ylabel='Crime Count')
    return ax


def plot_date_year_crime_id_count(df):
    """
    Plots the number of crimes by year.

    Args:
        df (pd.DataFrame): DataFrame containing crime data, including 'Date year' and 'Crime ID' columns.

    Returns:
        matplotlib.axes._subplots.AxesSubplot: A bar plot showing the number of crimes per year.
    """
    ax = sns.barplot(data=df.groupby('Date year').agg('count')[['Crime ID']], x=df.groupby('Date year').agg('count')[['Crime ID']].index, y='Crime ID')
    ax.set(ylabel='Crime Count')
    return ax


def create_year_month_crime_count_df(df):
    """
    Creates a DataFrame showing the crime count by date and crime type.

    Args:
        df (pd.DataFrame): DataFrame containing crime data, including 'Date' and 'Crime type' columns.

    Returns:
        pd.DataFrame: A DataFrame with the count of crimes per date, sorted by date.
    """
    year_month_crime_count_df = df.groupby(['Date', 'Crime type']).agg('count')[['Crime ID']].reset_index()
    year_month_crime_count_df = year_month_crime_count_df.sort_values(by='Date', ascending=True)
    return year_month_crime_count_df


def create_top_5_year_month_crime_count_df(df):
    """
    Filters the crime count DataFrame to include only the top 5 most common crime types over time.

    Args:
        df (pd.DataFrame): DataFrame containing crime data.

    Returns:
        pd.DataFrame: A DataFrame showing the count of the top 5 crime types over time.
    """
    top_5_crime_lst = create_top_5_crime_lst(df)
    year_month_crime_count_df = create_year_month_crime_count_df(df)
    top_5_year_month_crime_count_df = year_month_crime_count_df[year_month_crime_count_df['Crime type'].isin(top_5_crime_lst)]
    return top_5_year_month_crime_count_df


def plot_year_month_crime_count(df):
    """
    Plots the number of crimes by month and year, highlighting the top 5 crime types.

    Args:
        df (pd.DataFrame): DataFrame containing crime data.

    Returns:
        matplotlib.axes._subplots.AxesSubplot: A line plot showing the number of crimes per month and year.
    """
    top_5_year_month_crime_count_df = create_top_5_year_month_crime_count_df(df)
    plt.figure(figsize=(15, 6))
    ax = sns.lineplot(data=top_5_year_month_crime_count_df, x='Date', y='Crime ID', hue='Crime type')
    ax.set_xticklabels(ax.get_xticklabels(), rotation=45, ha="right")
    ax.axvline("2022-01-01", color="black", linestyle="--", label="2022")
    ax.axvline("2023-01-01", color="black", linestyle="--", label="2023")
    ax.axvline("2024-01-01", color="black", linestyle="--", label="2024")
    ax.set(ylabel='Crime Count')
    return ax


def create_date_location_hotspots_df(df):
    """
    Creates a DataFrame showing the count of crimes by location and date.

    Args:
        df (pd.DataFrame): DataFrame containing crime data, including 'Location' and 'Date' columns.

    Returns:
        pd.DataFrame: A pivot table with Locations as rows and Dates as columns, showing the count of crimes.
    """
    date_location_hotspots_df = df.groupby(['Location', 'Date']).size().unstack().fillna(0)
    return date_location_hotspots_df


def create_top_crime_location_lst(df):
    """
    Identifies the top locations with the highest number of reported crimes, excluding any location labeled 'No Info'.

    Args:
        df (pd.DataFrame): DataFrame containing crime data, including 'Location' and 'Crime ID' columns.

    Returns:
        pandas.Index: An index of the top locations with the most reported crimes.
    """
    top_crime_location_lst = df.groupby('Location').agg('count')[['Crime ID']].sort_values(by='Crime ID', ascending=False).drop(index='No Info').head().index
    return top_crime_location_lst


def plot_date_crime_count(df):
    """
    Plots the number of crimes over time for the top locations with the highest crime counts.

    Args:
        df (pd.DataFrame): DataFrame containing crime data.

    Returns:
        matplotlib.axes._subplots.AxesSubplot: A line plot showing crime trends over time for the top locations.
    """
    top_crime_location_lst = create_top_crime_location_lst(df)
    date_location_hotspots_df = create_date_location_hotspots_df(df)
    city_crime_hotspots_df_long = date_location_hotspots_df.reset_index().melt(id_vars='Location', var_name='Date', value_name='Crime Count')
    
    # Plot the data
    plt.figure(figsize=(15, 8))
    ax = sns.lineplot(data=city_crime_hotspots_df_long[city_crime_hotspots_df_long['Location'].isin(top_crime_location_lst)], x='Date', y='Crime Count', hue='Location', style='Location')
    ax.set_xticklabels(ax.get_xticklabels(), rotation=45, ha="right")
    ax.axvline("2022-01-01", color="black", linestyle="--", label="2022")
    ax.axvline("2023-01-01", color="black", linestyle="--", label="2023")
    ax.axvline("2024-01-01", color="black", linestyle="--", label="2024")
    return ax


def create_top_5_LSOA_name_df(df):
    """
    Identifies the top 5 LSOA (Lower Super Output Area) names with the highest number of reported crimes.

    Args:
        df (pd.DataFrame): DataFrame containing crime data, including 'LSOA name' and 'Crime ID' columns.

    Returns:
        pd.DataFrame: A DataFrame showing the top 5 LSOA names with the most reported crimes.
    """
    top_5_LSOA_name_df = df.groupby('LSOA name').count()[['Crime ID']].sort_values(by='Crime ID', ascending=False).head()
    return top_5_LSOA_name_df


def plot_LSOA_name_crime_count(df):
    """
    Plots the number of crimes for the top 5 LSOA (Lower Super Output Area) names.

    Args:
        df (pd.DataFrame): DataFrame containing crime data.

    Returns:
        matplotlib.axes._subplots.AxesSubplot: A bar plot showing crime counts for the top 5 LSOA names.
    """
    plt.figure(figsize=(15, 6))
    top_5_LSOA_name_df = create_top_5_LSOA_name_df(df)
    ax = sns.barplot(data=top_5_LSOA_name_df, x='LSOA name', y='Crime ID')
    ax.set_xticklabels(ax.get_xticklabels(), rotation=40, ha="right")
    ax.set(ylabel='Crime Count')
    ax.bar_label(ax.containers[0])
    return ax


def create_longitute_lantitude_crime_count_df(df):
    """
    Aggregates the crime count by longitude and latitude.

    Args:
        df (pd.DataFrame): DataFrame containing crime data, including 'Longitude' and 'Latitude' columns.

    Returns:
        pd.DataFrame: A DataFrame showing the count of crimes per unique combination of longitude and latitude.
    """
    longitute_lantitude_crime_count_df = df.groupby(['Longitude', 'Latitude']).size().reset_index(name='Crime ID')
    return longitute_lantitude_crime_count_df


def numeric_checked_longitute_lantitude_crime_count_df(df):
    """
    Ensures that longitude and latitude columns are numeric, and drops rows with missing values.

    Args:
        df (pd.DataFrame): DataFrame containing crime data, including 'Longitude', 'Latitude', and 'Crime ID' columns.

    Returns:
        pd.DataFrame: A cleaned DataFrame with numeric longitude and latitude, without missing values.
    """
    df['Latitude'] = pd.to_numeric(df['Latitude'], errors='coerce')
    df['Longitude'] = pd.to_numeric(df['Longitude'], errors='coerce')
    df = df.dropna(subset=['Latitude', 'Longitude', 'Crime ID'])
    return df


def plot_heatmap_crime_count(df):
    """
    Plots a heatmap of crime locations based on latitude and longitude.

    Args:
        df (pd.DataFrame): DataFrame containing crime data.

    Returns:
        folium.Map: A Folium map object with a heatmap layer displaying crime locations.
    """
    longitute_lantitude_crime_count_df = create_longitute_lantitude_crime_count_df(df)
    longitute_lantitude_crime_count_df = numeric_checked_longitute_lantitude_crime_count_df(longitute_lantitude_crime_count_df)
    map_center = [longitute_lantitude_crime_count_df['Latitude'].mean(), longitute_lantitude_crime_count_df['Longitude'].mean()]
    mymap = folium.Map(location=map_center, zoom_start=10)
    heat_data = [[row['Latitude'], row['Longitude'], row['Crime ID']] for index, row in longitute_lantitude_crime_count_df.iterrows()]
    HeatMap(heat_data).add_to(mymap)
    return mymap






