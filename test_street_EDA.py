import pytest
import pandas as pd
import numpy as np
from street_EDA import *

def test_create_top_5_crime_lst():
    data = {
        "Crime type": ["Theft", "Theft", "Assault", "Assault", "Assault", "Burglary", "Burglary", "Robbery", "Robbery", "Fraud"]
    }
    df = pd.DataFrame(data)
    
    result = create_top_5_crime_lst(df)
    expected = ["Assault", "Theft", "Burglary", "Robbery", "Fraud"]  # The crimes sorted by frequency
    
    assert result == expected, f"Expected {expected}, but got {result}"

def test_create_top_5_crime_df():
    data = {
        "Crime type": ["Theft", "Theft", "Assault", "Assault", "Assault", "Burglary", "Burglary", "Robbery", "Robbery", "Fraud"],
        "Other Data": range(10)
    }
    df = pd.DataFrame(data)
    
    result = create_top_5_crime_df(df)
    expected_crimes = ["Assault", "Theft", "Burglary", "Robbery", "Fraud"]
    
    assert result["Crime type"].isin(expected_crimes).all(), "DataFrame does not contain only the top 5 crimes"

def test_create_crime_count_year_month_df():
    data = {
        "Date": ["2023-01-01", "2023-01-01", "2023-02-01", "2023-02-01", "2023-03-01"],
        "Crime type": ["Theft", "Theft", "Assault", "Assault", "Burglary"],
        "Crime ID": [1, 2, 3, 4, 5]  # Ensure these are unique counts for the aggregation
    }
    df = pd.DataFrame(data)
    
    result = create_crime_count_year_month_df(df)
    
    # Check that the function correctly groups and counts crimes
    assert result.shape[0] == 3, f"The resulting DataFrame should have 3 rows, but got {result.shape[0]}."
    assert set(result["Crime type"].unique()) == {"Theft", "Assault", "Burglary"}, "Crime types do not match expected values."


def test_create_top_5_crime_count_year_month_df():
    data = {
        "Date": ["2023-01-01", "2023-01-01", "2023-02-01", "2023-02-01", "2023-03-01", "2023-03-01", "2023-04-01"],
        "Crime type": ["Theft", "Theft", "Assault", "Assault", "Burglary", "Burglary", "Fraud"],
        "Crime ID": [1, 2, 3, 4, 5, 6, 7]
    }
    df = pd.DataFrame(data)
    
    result = create_top_5_crime_count_year_month_df(df)
    
    assert result.shape[0] <= 5, "The resulting DataFrame should have rows only for the top 5 crimes."
    assert "Date" in result.columns and "Crime type" in result.columns, "The result does not have the required columns."

def test_numeric_checked_longitute_lantitude_crime_count_df():
    data = {
        "Longitude": ["-0.127758", "not a number", "-0.127758", np.nan],
        "Latitude": ["51.507351", "51.507351", np.nan, "51.507351"],
        "Crime ID": [1, 2, 3, 4]
    }
    df = pd.DataFrame(data)
    
    result = numeric_checked_longitute_lantitude_crime_count_df(df)
    
    assert result.shape[0] == 1, "The resulting DataFrame should only have rows with valid numeric longitude and latitude."

def test_loop_all_functions(mocker):
    mocker.patch("os.chdir")  # Mock os.chdir to avoid changing directories during testing
    mocker.patch("pandas.DataFrame.to_csv")  # Mock the to_csv method to avoid file creation

    data = {
        "Date": ["2023-01-01", "2023-01-01", "2023-02-01"],
        "Crime type": ["Theft", "Theft", "Assault"],
        "Crime ID": [1, 2, 3],
        "Location": ["Location1", "No Info", "Location1"],  # Include 'No Info' to test dropping logic
        "Longitude": ["-0.127758", "-0.127758", "-0.127758"],
        "Latitude": ["51.507351", "51.507351", "51.507351"],
        "LSOA name": ["LSOA1", "LSOA2", "LSOA1"]
    }
    df = pd.DataFrame(data)
    regions_dict = {"region_1": df}

    loop_all_functions(regions_dict)
    
    assert pd.DataFrame.to_csv.called, "to_csv should have been called to save the results."
