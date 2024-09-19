import logging
import os
from street_cleaning import *
from street_EDA import *
from postcode_and_price_cleaning import *

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Primary and staging steps

def staging():
    """
    Ingest the data, apply cleaning, and store to CSV files for primary.
    """
    logging.info("Starting staging process...")

    # Ingest raw data
    street_regional_dic = combined_dataset('street')
    logging.info("Raw data ingested.")

    # Dropping the 'Context' column for all DataFrames
    for key, value in street_regional_dic.items():
        value.drop('Context', axis=1, inplace=True)
    logging.info("Context column dropped.")

    drop_rows(street_regional_dic, ['Longitude', 
                                    'Latitude', 
                                    'Crime ID', 
                                    'Last outcome category',
                                    'LSOA code', 'LSOA name'])
    logging.info("Specified columns dropped.")

    # Dropping duplicates for 'Crime ID' column.
    for key, value in street_regional_dic.items():
        value.drop_duplicates(subset='Crime ID', inplace=True)
    logging.info("Duplicates dropped based on 'Crime ID'.")

    # Making the directory to store the staging data if it doesn't exist
    try:
        os.makedirs('staged_dataframe')
        logging.info("Directory 'staged_dataframe' created.")
    except FileExistsError:
        logging.info("Directory 'staged_dataframe' already exists.")

    # Save the staged DataFrame as CSV in staged_dataframe
    os.chdir('staged_dataframe')
    for key, value in street_regional_dic.items():
        value.to_csv(f'staged_{key}')
    os.chdir('..')
    logging.info("Staged DataFrames saved to 'staged_dataframe'.")

    # UK postcode
    try:
        uk_post_df = read_and_clean_uk_postcode()
        logging.info("UK postcode data read and cleaned.")
    except Exception as e:
        logging.error(f"Failed to read and clean UK postcode data: {e}")

    return

def primary():
    """
    Store the transformed data to CSV files.
    """
    logging.info("Starting primary process...")

    # Reading the staged CSV for each location as a dictionary
    staged_csv_dict = read_pipeline_csv_to_dict('staged')
    logging.info("Staged CSVs read into dictionary.")

    # Separate yyyy-mm to 2 columns: yyyy and mm
    for key, value in staged_csv_dict.items():
        staged_csv_dict[key] = convert_y_m(value)
    logging.info("Converted yyyy-mm to separate yyyy and mm columns.")

    no_or_near_replace(staged_csv_dict)
    logging.info("Replaced 'no' or 'near' values.")

    dic_apply_categorization(staged_csv_dict)
    logging.info("Applied categorization to the DataFrames.")

    # Making the directory to store the primary data if it doesn't exist
    try:
        os.makedirs('primary_dataframe')
        logging.info("Directory 'primary_dataframe' created.")
    except FileExistsError:
        logging.info("Directory 'primary_dataframe' already exists.")

    # Save the primary DataFrame as CSV in primary_dataframe
    os.chdir('primary_dataframe')
    for key, value in staged_csv_dict.items():
        value.to_csv(f'primary_{key.split("_")[1]}_df')
    os.chdir('..')
    logging.info("Primary DataFrames saved to 'primary_dataframe'.")

    # Postcode analysis, merging the postcode df to the street df
    try:
        for key, value in staged_csv_dict.items():
            merge_coordinate_df(key, value)
        logging.info("Postcode data merged with street data.")
    except Exception as e:
        logging.error(f"Failed to merge postcode data with street data: {e}")

    # Pricing analysis
    try:
        create_pp_df()
        logging.info("Pricing analysis completed.")
    except Exception as e:
        logging.error(f"Failed to complete pricing analysis: {e}")

    return

# Reporting
def reporting():
    """
    Reporting Layer: Store the aggregated reporting data to CSV files.
    """
    logging.info("Starting reporting process...")

    # Making the directory to store the reporting data if it doesn't exist
    try:
        os.makedirs('reporting_dataframe')
        logging.info("Directory 'reporting_dataframe' created.")
    except FileExistsError:
        logging.info("Directory 'reporting_dataframe' already exists.")

    primary_dict = read_pipeline_csv_to_dict('primary')
    logging.info("Primary CSVs read into dictionary.")

    loop_all_functions(primary_dict)
    logging.info("Aggregated data processed for reporting.")

    return

def main(pipeline_start='staging', pipeline_goal='all'):
    """
    The function performs the pipeline action for the selected data.
    The order of execution should be 'staging' -> 'primary' -> 'reporting' -> 'all'.
    The pipeline_goal CANNOT be before pipeline_start, e.g., pipeline_start='reporting', pipeline_goal='primary' is not allowed.
    """
    logging.info('Pipeline Execution Started.')
    logging.info(f'Data Layer Start: {pipeline_start}')
    logging.info(f'Data Layer Goal: {pipeline_goal}')

    pipeline_order = ['staging', 'primary', 'reporting', 'all']

    try:
        if pipeline_start not in pipeline_order or pipeline_goal not in pipeline_order:
            raise ValueError("Invalid pipeline_start or pipeline_goal specified. Please choose 'staging', 'primary', 'reporting', 'all'.")

        if pipeline_order.index(pipeline_start) > pipeline_order.index(pipeline_goal):
            raise ValueError("pipeline_goal cannot be before pipeline_start.")

        if pipeline_start == 'staging':
            staging()
            logging.info('Staging Completed')
            if pipeline_goal == 'staging':
                logging.info(f'Target Pipeline: {pipeline_goal} Reached')
                return

        if pipeline_start in ['staging', 'primary']:
            primary()
            logging.info('Primary Completed')
            if pipeline_goal == 'primary':
                logging.info(f'Target Pipeline: {pipeline_goal} Reached')
                return

        if pipeline_start in ['staging', 'primary', 'reporting']:
            reporting()
            logging.info('Reporting Completed')
            if pipeline_goal == 'reporting':
                logging.info(f'Target Pipeline: {pipeline_goal} Reached')
                return

        if pipeline_goal == 'all':
            logging.info(f'Target Pipeline: {pipeline_goal} Reached')

    except Exception as e:
        logging.critical(f'Pipeline execution failed: {e}')

    return

main()
