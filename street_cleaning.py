import pandas as pd
import os

def extract_city_name_from_file():
    """
    Args:
    The function takes no Args from user, it goes into police data folders and read the names of the files as input.

    Returns:
    Region names extract from all the files within a folder.
    """
    os.chdir(os.listdir()[0])
    file_list = os.listdir() #listing out all the file names.

    regions = [filename.split('-')[2:-1] for filename in file_list]  # spliting and extracting the region names.
    regions = ['-'.join(region) for region in regions] #join region names together.
    os.chdir('../')

    return regions

def combined_dataset(dataset_type):
    """
    Args:
    dataset_type (str): the type of data e.g. 'street', 'stop-and-search', 'outcomes'

    Returns:
    The function takes in these arguments and produces a dictionary with region names as keys, and their associated dataframe as values.
    These dataframe are the product of combining the dataset of all the months for each region.
    The function will also print out the files that were not found or caused errors.
    """
    # reset current directory
    os.chdir('police_data')

    #list of folders within police_data
    folder_names_ls = os.listdir()

    #Constable names
    regional_constables_ls = extract_city_name_from_file()
    
    # initialize the dictionary to store DataFrames for each region
    regional_dic = {}
    
    # initialize the list to store failed attempts
    failed_ls = []
    
    # loop through each region in the list
    for region in regional_constables_ls:
        # initialize an empty DataFrame for the region if it doesn't exist in the dictionary
        regional_dic[f'{region}_df'] = pd.DataFrame()
        
        # loop through each folder name
        for name in folder_names_ls:
            try:
                # change to the specific folder
                os.chdir(f'{name}')
                
                # construct the file path and read the CSV file
                file_path = f'{name}-{region}-{dataset_type}.csv'
                new_data = pd.read_csv(file_path)
                
                # concatenate the new data with the existing DataFrame for the region
                regional_dic[f'{region}_df'] = pd.concat([regional_dic[f'{region}_df'], new_data], ignore_index=True)
            except FileNotFoundError:
                # append to the failed list if the file is not found
                failed_ls.append(f'{name}-{region}')
            except Exception as e:
                # handle other exceptions and print an error message
                print(f"An error occurred with {name}-{region}: {e}")
                failed_ls.append(f'{name}-{region}')
            finally:
                # return to the previous directory
                os.chdir('..')
    
    # print the list of failed attempts
    if failed_ls:
        print("The following files were not found or caused errors:")
        print(failed_ls)

    os.chdir('..')
    
    return regional_dic

def drop_rows(dic,column):
    """
    Args:
    dic(dict) = dictionary, where the keys are the df names, and the values are the dataframe objects.
    column(lst) = the name of the column or columns you wish to base your row deletion on.

    Returns:
    The function drops the rows that have NaN value in the user column or columns.
    This is performed for all the dataframes (values) in the dictionary.
    """
    for key, value in dic.items():
        try:
            for c in column:
                value.dropna(subset= c, inplace=True)
        except:
            pass
    return dic

def convert_y_m(df):
    """
    Converts a 'Month' column in the format 'YYYY-MM' into separate 'Date year' and 'Date month' columns,
    and renames 'Month' to 'Date' after converting it to a datetime object.

    Args:
    df(pd.DataFrame): The input DataFrame containing at least a 'Month' column in 'YYYY-MM' format.

    Returns:
    A df with a separate 'Date year' and 'Date month' columns, extracted from the original 'Month' column.
    """
    temp_df = df.join(df['Month'].str.split('-', n=2, expand=True).rename(columns={0:'Date year', 1:'Date month'}))
    temp_df['Month'] = pd.to_datetime(temp_df['Month'], format='%Y-%m')
    temp_df.rename({'Month':'Date'}, axis=1, inplace=True)
    return temp_df

def covert_y_m_dic(dic):
    """
    Args:
    dic (dict): A dictionary where each key is associated with a DataFrame. Each DataFrame must contain a 'Month' column in 'YYYY-MM' format.

    Returns:
    All df from the dic will be converted. 
    """
    for key, value in street_regional_dic.items():
        dic[key] = convert_y_m(value)
    return dic

def no_or_near_replace(dic):
    """
    Args:
    dic(dict): the name of the dictionary that contains the dataframes as values.

    Returns:
    Changes all the 'On or Near' strings in the 'Location' column to 'N Info', for all the DataFrames
    """
    for key, value in dic.items():
        if isinstance(value, pd.DataFrame) and 'Location' in value.columns:
            value['Location'] = value['Location'].apply(lambda x: 'No Info' if str(x).strip().lower() == 'on or near' else x)
    return dic

def categorize_outcome(outcome):
    """
    Args:
    outcome(str): The values in the outcome column.

    Returns:
    The categorised outcomes.
    """
    if outcome in ['Unable to prosecute suspect', 
                   'Investigation complete; no suspect identified', 
                   'Status update unavailable']:
        return 'No Further Action'
    elif outcome in ['Local resolution', 
                     'Offender given a caution', 
                     'Action to be taken by another organisation'
                     ]:
        return 'Non-criminal Outcome'
    elif outcome in ['Further investigation is not in the public interest', 
                     'Further action is not in the public interest', 
                     'Formal action is not in the public interest']:
        return 'Not in Public Interest Consideration'
    else:
        return outcome  # Or any other category

def apply_categorization(df):
    """
    Args:
    df(pd.DataFrame): police dataframe.

    Returns:
    Apply categorisation to 'Final Outcome' column.
    """
    df['Broad Outcome Category'] = df['Last outcome category'].apply(categorize_outcome)
    return df

def dic_apply_categorization(dic):
    """
    Args:
    dic(dict): the name of the dictionary that contains the dataframes as values.
    
    Returns:
    Apply categorisation to the dictionary containing dataframe.
    """
    for key, value in dic.items():
        dic[key] = apply_categorization(value)
    return dic


def read_pipeline_csv_to_dict(step):
    """
    Args:
    step(str): Stage of the pipeline:'staged', 'primary'.
    
    Returns:
    A dictionary containing the region as the key, and the respetive dataframes as values.
    """
    os.chdir(f'{step}_dataframe/')
    staged_dict = {}

    for file in os.listdir():
        staged_dict[f'{file}'] = pd.read_csv(file, index_col=0)
    os.chdir('../')

    return staged_dict
