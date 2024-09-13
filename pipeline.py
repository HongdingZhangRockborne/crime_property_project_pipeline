from street_cleaning import *
from street_EDA import *

#Primary and staging steps


def staging():
    """
    Ingest the data, apply cleaning, and store to CSV files for primary.
    """
    # ingest rawd data
    street_regional_dic = combined_dataset('street')

    #dropping the 'Context' column for all DataFrames
    for key, value in street_regional_dic.items():
        value.drop('Context', axis=1, inplace=True)

    drop_rows(street_regional_dic, ['Longitude', 
                                    'Latitude', 
                                    'Crime ID', 
                                    'Last outcome category',
                                    'LSOA code','LSOA name'])
    
    #Dropping duplciates for 'Crime ID' column.
    for key, value in street_regional_dic.items():
        value.drop_duplicates(subset='Crime ID', inplace=True)

    #Making the directory to store the staging data if they don't exist
    try:
        os.makedirs('staged_dataframe')
    except:
        pass
    
    #save the staged df as csv in staged_dateframe
    os.chdir('staged_dataframe')
    for key, value in street_regional_dic.items():
        value.to_csv(f'staged_{key}')
    os.chdir('..')
        
    return

def primary():
    """
    Store the transformed data to a CSV files.
    """
    #reading the staged csv for each location as a dictionary
    staged_csv_dict = read_pipeline_csv_to_dict('staged')

    
    #seperate yyyy-mm to 2 columns: yyyy and mm
    for key, value in staged_csv_dict.items():
        
        staged_csv_dict[key] = convert_y_m(value)

    no_or_near_replace(staged_csv_dict)

    dic_apply_categorization(staged_csv_dict)
    
    #Making the directory to store the primary data if they don't exist
    try:
        os.makedirs('primary_dataframe')
    except:
        pass
    
    #save the staged df as csv in staged_dateframe
    os.chdir('primary_dataframe')
    for key, value in staged_csv_dict.items():
        value.to_csv(f'primary_{key.split('_')[1]}_df')
    os.chdir('..')

    return

#reporting
def reporting():
    """
    Reporting Layer: Store the aggregated reporting data to a CSV files.
    """
    # TODO: Implement reporting aggregation - Example aggregation: Count of crimes by crime type and broad outcome category
    try:
        os.makedirs('visualisation_dataframe')
    except:
        pass
    
    primary_dict = read_pipeline_csv_to_dict('primary')
    
    loop_all_functions(primary_dict)
    
    return

staging()
primary()
reporting()