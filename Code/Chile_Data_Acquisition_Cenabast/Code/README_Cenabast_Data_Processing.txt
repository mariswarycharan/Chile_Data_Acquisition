README:  Chile Data Processing Pipeline(Cenabast)
Overview
This pipeline is designed to process, clean, and enrich pharmaceutical data for Chile. It integrates multiple functionalities, including fuzzy matching, mapping, data standardization, and Cenabast data acquisition. The goal is to produce comprehensive and standardized datasets for analytical and reporting purposes.
Project Structure

Chile.py:
Main orchestration script.
Calls functions from part_1.py and part_2.py to process Cenabast data.
Outputs enriched datasets.

Key Function
	Chile_Data_Acquisition_Cenabast_data(Url_Month_turple):
		Parameters:
			Url_Month_turple: A tuple containing:
			Cenabast_File_Url: URL to fetch the Excel file.
			Month: Month identifier for filtering data.
	Output:
		final_data: Comprehensive processed data.
		final_Combinacion_Vertical: Dataset for vertical reporting.
	Steps:
		Extracts the dataset from the provided URL.
		Filters the data by the specified month.
		Applies fuzzy matching and formatting functions.
		Outputs processed DataFrames for further analysis.
Workflow
	Input:
		Accepts a tuple containing:
		Cenabast_File_Url: URL to fetch the dataset.
		Month: Month identifier for filtering.
	Data Acquisition:
		Fetches the dataset from the provided URL.
		Downloads the file as an Excel sheet and reads it into a Pandas DataFrame.
	Data Processing:
		Filters the dataset by the specified month.
		Applies fuzzy matching and product cleansing using the fuzzy_match_filtering function.
		Maps and formats data using the Mapping_File_Format function.
	Output:
		Produces two processed datasets:
			final_data: Comprehensive processed data.
			final_Combinacion_Vertical: Summarized dataset for vertical reporting.

part_1.py:
Implements fuzzy matching for product descriptions.
Extracts and maps dosages using regex and predefined rules.
Ensures accuracy and consistency in product data.

Key Function
	fuzzy_match_filtering(dataframe, ini_forget_time)

	Parameters:
			dataframe: Input DataFrame to process.
			ini_forget_time: Timestamp string for log file naming.

	Steps:
		Loads the market basket file and filters data for active ingredients and brands.
		Matches product descriptions to predefined lists using fuzzy matching algorithms.
		Extracts dosage details and ensures data consistency.
	Output:
		Processed DataFrame with enriched and standardized fields.
Workflow
	Input:
		Accepts a Pandas DataFrame containing product descriptions and details.
		Requires a market basket file (Control/New_Market_Basket.xlsx) for predefined mappings.
	Fuzzy Matching:
		Matches product descriptions (Nombre producto genérico and Nombre producto comercial) to active ingredients (Pactivo) and brand names using fuzzy logic.
		Filters match by a confidence threshold (default: score_cutoff=81).
	Dosage Processing:
		Extracts dosage information using regex patterns.
		Removes duplicates and standardizes the dosage format.
	Output:
		Returns a cleaned and enriched DataFrame with additional columns, such as Pactivo_Final, Brand_Final, and Dosage_Final.

part_2.py:
Handles buyer and supplier mappings using Cenabast mapping files.
Formats and localizes data fields (e.g., regions, months).
Produces vertically integrated datasets for institutional reporting.

Key Functions
	calculate_rut_check_digit(rut)
		Purpose: Validates and appends RUT check digits.
		Parameters:
				rut: Integer representing the RUT number.
		Output: String with the formatted RUT and check digit.

	Mapping_File_Format(dataframe)
	Purpose: Processes and formats the dataset using mapping files.
		Steps:
			Calculates RUT check digits and appends them to buyer fields.
			Maps buyers, suppliers, and institutional recipients.
			Formats fields, including dates, regions, and product data.
		Generates two outputs dataframes:
			final_data: Enriched dataset.
			final_Combinacion_Vertical: Summarized reporting dataset.

Workflow
	Input:
		Accepts a Pandas DataFrame containing raw data.
		Requires mapping files for buyers, suppliers, and institutions.
	Data Processing:
		Calculates RUT check digits for buyer fields.
		Maps buyers, suppliers, and institutional recipients using Excel-based mapping files.
		Formats dates, regions, and product information for consistency.
	Output:
		Returns two processed DataFrames:
		final_data: Comprehensive dataset with all mappings.
		final_Combinacion_Vertical: Summarized dataset for specific reporting purposes.


