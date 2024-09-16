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




