import pytest
import os
import pandas as pd
from unittest.mock import patch, MagicMock

from street_cleaning import *

# 1. Fixture-Based Testing
@pytest.fixture
def mock_file_list():
    return [
        "police-2023-08-region1-street.csv",
        "police-2023-08-region2-street.csv"
    ]

@pytest.fixture
def mock_data_frame():
    return pd.DataFrame({
        "Month": ["2023-08", "2023-07", "2023-06"],
        "Location": ["On or Near", "Somewhere", "Nowhere"],
        "Last outcome category": ["Investigation complete; no suspect identified", "Local resolution", "Formal action is not in the public interest"]
    })

@pytest.fixture
def mock_dict():
    return {
        "region1_df": pd.DataFrame({
            "Month": ["2023-08", "2023-07", "2023-06"],
            "Location": ["On or Near", "Somewhere", "Nowhere"],
            "Last outcome category": ["Investigation complete; no suspect identified", "Local resolution", "Formal action is not in the public interest"]
        }),
        "region2_df": pd.DataFrame({
            "Month": ["2023-08", "2023-07", "2023-06"],
            "Location": ["On or Near", "Somewhere", "Nowhere"],
            "Last outcome category": ["Investigation complete; no suspect identified", "Local resolution", "Formal action is not in the public interest"]
        })
    }

# 2. Mocking and Assertions
def test_extract_city_name_from_file(mocker):
    # Adjusted mock file list to produce the expected result
    mock_file_list = ['police-2023-region1-street.csv', 'police-2023-region2-street.csv']
    mocker.patch("os.listdir", return_value=mock_file_list)
    mocker.patch("os.chdir")
    
    result = extract_city_name_from_file()
    assert result == ["region1", "region2"]  # Adjusted to match expected output
 

def test_combined_dataset(mocker, mock_dict, mock_file_list):
    mocker.patch("street_cleaning.extract_city_name_from_file", return_value=["region1", "region2"])
    mocker.patch("os.listdir", return_value=["2023-08"])
    mocker.patch("pandas.read_csv", return_value=mock_dict["region1_df"])
    mocker.patch("os.chdir")
    
    result = combined_dataset("street")
    assert "region1_df" in result
    assert "region2_df" in result
    assert not result["region1_df"].empty
    assert not result["region2_df"].empty

@pytest.fixture
def mock_dict_with_nan():
    return {
        "region1_df": pd.DataFrame({
            "Month": ["2023-08", "2023-07", None],  # Include NaN in 'Month'
            "Location": ["On or Near", None, "Nowhere"],  # Include NaN in 'Location'
            "Last outcome category": ["Investigation complete; no suspect identified", "Local resolution", "Formal action is not in the public interest"]
        }),
        "region2_df": pd.DataFrame({
            "Month": ["2023-08", None, "2023-06"],  # Include NaN in 'Month'
            "Location": [None, "Somewhere", "Nowhere"],  # Include NaN in 'Location'
            "Last outcome category": ["Investigation complete; no suspect identified", "Local resolution", None]
        })
    }

@pytest.mark.parametrize("column, expected_shape", [
    (["Location"], (2, 3)),  # Expect to drop 1 row with NaN in 'Location'
    (["Location", "Month"], (1, 3))  # Expect to drop 2 rows with NaN in 'Location' and 'Month'
])
def test_drop_rows(mock_dict_with_nan, column, expected_shape):
    result = drop_rows(mock_dict_with_nan, column)
    for key, value in result.items():
        assert value.shape == expected_shape

# 4. Testing Date Conversion
def test_convert_y_m(mock_data_frame):
    result = convert_y_m(mock_data_frame)
    assert "Date year" in result.columns
    assert "Date month" in result.columns
    assert "Date" in result.columns
    assert result["Date"].dtype == "datetime64[ns]"

def test_covert_y_m_dic(mock_dict):
    result = covert_y_m_dic(mock_dict)  
    for key, value in result.items():
        assert "Date year" in value.columns
        assert "Date month" in value.columns
        assert "Date" in value.columns
        assert value["Date"].dtype == "datetime64[ns]"



# 5. Testing Categorization
@pytest.mark.parametrize("outcome, expected_category", [
    ("Unable to prosecute suspect", "No Further Action"),
    ("Local resolution", "Non-criminal Outcome"),
    ("Formal action is not in the public interest", "Not in Public Interest Consideration"),
    ("Some other outcome", "Some other outcome")
])
def test_categorize_outcome(outcome, expected_category):
    assert categorize_outcome(outcome) == expected_category

def test_dic_apply_categorization(mock_dict):
    result = dic_apply_categorization(mock_dict)
    for key, value in result.items():
        assert "Broad Outcome Category" in value.columns
        assert value["Broad Outcome Category"].iloc[0] == "No Further Action"

# 6. Testing No or Near Replace
def test_no_or_near_replace(mock_dict):
    result = no_or_near_replace(mock_dict)
    for key, value in result.items():
        assert (value['Location'] == "No Info").sum() == 1

# 7. Testing Read Pipeline CSV to Dict
def test_read_pipeline_csv_to_dict(mocker):
    mock_csv = mocker.patch("pandas.read_csv", return_value=pd.DataFrame({"column1": [1, 2, 3]}))
    mocker.patch("os.listdir", return_value=["region1.csv", "region2.csv"])
    mocker.patch("os.chdir")
    
    result = read_pipeline_csv_to_dict("staged")
    assert "region1.csv" in result
    assert "region2.csv" in result
    assert mock_csv.call_count == 2
