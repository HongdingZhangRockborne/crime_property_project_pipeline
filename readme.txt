This folder can be downloaded and used for crime data analysis. 

This folder is designed to be used to cover the data pipe line from cleaning to producing dataframes that can be directly use.
It also implements creating postcode analysis and house price analysis.

These tool are created to be easy to use, but please follow the intruction carefully.

1.Download the datasets:

- Police Crime data can be downloaded via https://data.police.uk/data/
	* The pipeline is designed to take in data from any police force and any year/month.
	* Create a folder named 'police_data'.
  	* The dowloaded Police Crime data should be stored in the folder 'police_data'.
	* This can be done by copying all the folders in the downloaded zip file into the police_data folder.

- UK postcode and coordinate data can be downloaded via https://www.freemaptools.com/download-uk-postcode-lat-lng.htm
	* Create a folder named 'uk_postcode'.
	* Download the CSV file and store it in the folder named 'uk_postcode'.

- Property-sold data can be downloaded via https://www.gov.uk/government/statistical-data-sets/price-paid-data-downloads#single-file
	* Create a folder named 'properties_sold'.
	* Make sure to download the CSV files for the years you are interested.
	* For the year 2024, simply download the 'current month as a CSV file' under the 'July 2024 data (current month)', the code will do the rest.
	* Store the datasets in the folder named 'properties_sold'.

2. Running the pipeline:
Before running the pipeline, please install the following packages using 'pip install *', where the * any of the following:
- pandas
- numpy
- os
- logging
- pytest
- pytest-mock

Simply open the terminal via whichever software, but make sure its 'cmd'. Go to where this folder is located and cd into this folder.
To run the pipeline, simply type in 'python pipeline.py' and hit enter.

3. Unit testing:
There are several test_*.py file designed to test the functions built, and they should test the function in the correspoding *.py file.
To run the test, use the command 'pytest *' where * is the name of the test file. Then just hit enter.

4. Expected output files:

- Data that has went through the staging stage will be stored in the folder named 'staged_dataframe'.
- Data that has went through the primary stage will be stored in the folder named 'primary_dataframe'.
- Data that has went through the reporting stage will be stored in the folder named 'reporting_dataframe'

exceptions:
- Cleaned UK postcode data will be stored in the 'uk_postcode' folder.
- Cleaned property sold data that are combined into a single df, and it is stored in the 'properties_sold' folder.
- Post_code_staged_*_df are street dataframes with a added colomn of postcode, and they are stored in the folder named 'post_code_street'.


Happy pipelining :)

Ding

