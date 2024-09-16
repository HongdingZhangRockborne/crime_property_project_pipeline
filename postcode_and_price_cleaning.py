import street_cleaning as cf
import os
import pandas as pd

def remove_uk_post_duplicate(uk_post_df):
    """
    The duplciates are dropped for the rounded uk_post df, so there wont be cross joints during merging with the street df.
    """
    uk_post_df = uk_post_df.drop_duplicates(subset=['Latitude_4dp', 'Longitude_4dp'])

    return uk_post_df

def read_and_clean_uk_postcode():
    """
    Function reads and cleans the uk postcode data, then store it as a new csv.
    """
    os.chdir('uk_postcode') #entering the directory that saved the ukpostcode data.
    uk_post_df = pd.read_csv('ukpostcodes.csv')
    uk_post_df.columns = ['ID', 'Postcode', 'Latitude', 'Longitude']
    uk_post_df = uk_post_df[['Postcode', 'Latitude', 'Longitude']]
    uk_post_df.dropna(subset= ['Latitude','Longitude'], inplace=True)

    uk_post_df.to_csv('cleaned_ukpostcodes') #saving the cleaned df as a new csv.
    os.chdir('..') #return to the main folder.
    
    return 

def rounding_lon_lat_3dp(coordinate_df):
    """
    Args:
    coordinate_df (pandas.DataFrame()): The dataframe containing coordinates. 

    Returns:
    The coordinate_df with rounded df (3.d.p, which is an acurracy of 111.1 m).
    """
    coordinate_df['Latitude_4dp'] = coordinate_df['Latitude'].round(3)
    coordinate_df['Longitude_4dp'] = coordinate_df['Longitude'].round(3)

    return coordinate_df

def merge_coordinate_df(street_df_name, street_df):
    """
    Args:
    street_df (pandas.DataFrame()): the street police df

    Returns:
    A merged df. And saved as post_code_street in a new directory.
    """
    try:
        os.makedirs('post_code_street')
    except:
        pass
    os.chdir('uk_postcode') #getting the uk postcode data.
    uk_post_df = pd.read_csv('cleaned_ukpostcodes',index_col=0)
    os.chdir('..') #back to the main folder.

    merged_df = pd.merge(rounding_lon_lat_3dp(street_df), 
                         remove_uk_post_duplicate(rounding_lon_lat_3dp(uk_post_df)), 
                         how='inner', 
                         left_on=['Latitude_4dp', 'Longitude_4dp'], 
                         right_on=['Latitude_4dp', 'Longitude_4dp'])
    
    os.chdir('post_code_street')
    merged_df.to_csv(f'post_code_{street_df_name}')
    os.chdir('..')

    return 

def read_pp_df(file_name):
    """
    file_name: str, name of the CSV file
    Function create a dataframe form the file name (str), then produces the DataFrame with the designated column names.
    """
    pp_column_names = pp_column_names = ['Transaction unique identifier',
                  'Price',
                  'Date of Transfer',
                  'Postcode',
                  'Property Type',
                  'Old/New',
                  'Duration',
                  'PAON',
                  'SAON',
                  'Street',
                  'Locality',
                  'Town/City',
                  'District',
                  'County',
                  'PPD Category Type',
                  'Record Status - monthly file only']
    return pd.read_csv(file_name, names=pp_column_names)

def pp_keep_specified_columns(df):
    """
    df: pp DataFrame
    The function takes in the pp df and only keeps the relevant columns
    """
    df = df[['Transaction unique identifier',
         'Price',
         'Date of Transfer',
         'Postcode',
         'Property Type',
         'Old/New',
         'Street',
         'Duration']]
    return df

def pp_replace_street(df):
    """
    df: pp DataFrame
    Function replaces the NaN values in the 'Street' column to 'Street Not Availible'
    """
    df['Street'] = df['Street'].fillna('Street Not Availible')
    return df

def pp_property_type_full_name(df):
    """
    df: pp DataFrame
    Function takes in the pp df and replace the property type innitials to full name.
    """
    df['Property Type'] = df['Property Type'].apply(lambda x: 'Detached' if x == 'D' else 
                                         ('Semi-Detached' if x == 'S' else 
                                         ('Terraced' if x == 'T' else 
                                         ('Flats/Maisonettes' if x == 'F' else 'Other'))))
    return df

def pp_old_new_full_name(df):
    """
    df: pp DataFrame
    The function takes in the pp df and returns the 'Old/New' with non-abbreviated form.
    """
    df['Old/New'] = df['Old/New'].apply(lambda x : 'New' if x == 'N' else ('Old' if x == 'O' else 'Other'))
    return df

def pp_to_date_format(df):
    """
    df: pp DataFrame
    The function takes in the pp df and returns the Date of Transfer to date type.
    """
    df['Date of Transfer'] = pd.to_datetime(df['Date of Transfer'], format='%Y-%m-%d %H:%M')

    return df

def pp_duration_full_name(df):
    """
    df: panda.DataFrame for pp
    Function returns the non-abbreviated duration values. 
    """
    df['Duration'] = df['Duration'].apply(lambda x : 'Freehold' if x == 'F' else ('Leasehold' if x == 'L' else 'Other'))
    return df

def create_pp_df():
    """
    pp = Postcode Price
    This cod only works until 2024.
    """
    os.chdir('properties_sold')
    cleaned_all_year_pp_df = pd.DataFrame()

    file_lst = os.listdir()
    for f in file_lst:
        if f == 'pp-monthly-update-new-version':
            pp = read_pp_df('pp-monthly-update-new-version')
            pp_to_date_format(pp)
            pp = pp[pp['Date of Transfer'].dt.year == 2024]
        else:
            pp = read_pp_df(f)
            pp_to_date_format(pp)

        pp = pp_keep_specified_columns(pp)
        pp = pp.dropna(subset='Postcode')
        pp_replace_street(pp)
        pp_property_type_full_name(pp)
        pp_old_new_full_name(pp)
        pp_duration_full_name(pp)

        cleaned_all_year_pp_df = pd.concat([cleaned_all_year_pp_df, pp], ignore_index=True)

    
    cleaned_all_year_pp_df.to_csv('cleaned_all_year_pp_df')

    os.chdir('../')

    return


