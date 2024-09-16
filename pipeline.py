from street_cleaning import *
from street_EDA import *
from postcode_and_price_cleaning import *

#Primary and staging steps


def staging():
    """
    Ingest the data, apply cleaning, and store to CSV files for primary.
    """
    # ingest raw data
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


    #uk postcode
    try:
        uk_post_df = read_and_clean_uk_postcode()
    except:
        pass

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

    #Postcode analysis, merging the postcode df to the street df
    try:
        for key, value in staged_csv_dict.items():
            merge_coordinate_df(key, value)
    except:
        pass

    #pricing analysis:

    try:
        create_pp_df()
    except:
        pass

    return

#reporting
def reporting():
    """
    Reporting Layer: Store the aggregated reporting data to a CSV files.
    """
    # TODO: Implement reporting aggregation - Example aggregation: Count of crimes by crime type and broad outcome category
    try:
        os.makedirs('reporting_dataframe')
    except:
        pass
    
    primary_dict = read_pipeline_csv_to_dict('primary')
    
    loop_all_functions(primary_dict)
    
    return

def main(pipeline_start='staging', pipeline_goal='all'):
    """
    The function performs the pipeline action for the selected data.
    The order of execution should be 'staging' -> 'primary' -> 'reporting' -> 'all'.
    The pipeline_goal CANNOT be before pipeline_start, e.g., pipeline_start='reporting', pipeline_goal='primary' is not allowed.
    """
    print('Pipeline Execution Started.')
    print(f'Data Layer Start: {pipeline_start}')
    print(f'Data Layer Goal: {pipeline_goal}')

    pipeline_order = ['staging', 'primary', 'reporting', 'all']

    try:
        if pipeline_start not in pipeline_order or pipeline_goal not in pipeline_order:
            raise ValueError("Invalid pipeline_start or pipeline_goal specified. Please choose 'staging', 'primary', 'reporting', 'all'.")

        if pipeline_order.index(pipeline_start) > pipeline_order.index(pipeline_goal):
            raise ValueError("pipeline_goal cannot be before pipeline_start.")

        if pipeline_start == 'staging':
            staging()
            print('Staging Completed')
            if pipeline_goal == 'staging':
                print(f'Target Pipeline: {pipeline_goal} Reached')
                return

        if pipeline_start in ['staging', 'primary']:
            primary()
            print('Primary Completed')
            if pipeline_goal == 'primary':
                print(f'Target Pipeline: {pipeline_goal} Reached')
                return

        if pipeline_start in ['staging', 'primary', 'reporting']:
            reporting()
            print('Reporting Completed')
            if pipeline_goal == 'reporting':
                print(f'Target Pipeline: {pipeline_goal} Reached')
                return

        if pipeline_goal == 'all':
            print(f'Target Pipeline: {pipeline_goal} Reached')

    except Exception as e:
        print(f'ERROR: {e}')

    return

main()