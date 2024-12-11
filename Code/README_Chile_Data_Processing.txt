README: Chile Data Processing Pipeline
Overview
This project comprises a Python-based data processing pipeline designed for managing and analyzing pharmaceutical data from Chile. The workflow includes data acquisition, cleaning, fuzzy matching, and enhanced reporting functionalities. The pipeline leverages advanced Python libraries and parallel processing for scalability and efficiency.


File Descriptions

File Name: Chile.py
The main script orchestrates the workflow.
Imports functions from part_1, part_2, and part_3.
Executes the data download and cleaning pipeline.
Outputs a completion message upon success.

File Name: part_1.py
Primary Functions:
	download_data: Automates the download of ZIP files based on the configuration in the control file. Extracts and saves the content into specified directories.
	verify_columns: Validates the presence of required columns in the input data.
	clean_column: Standardizes text by removing whitespaces, tabs, and newline characters.
	process_csv: Processes individual CSV files, including type conversions, fuzzy matching, and price verification.
	Cleaning_Data: Orchestrates the cleaning process for all downloaded files and integrates mapping logic.
	save_csv_to_s3: Saves processed files to an Amazon S3 bucket (if credentials are provided).
	Also, this File connects Chile_Data_Acquisition_Cenabast for Cenabast data processing and merging data with Mercado Publico output. 
Output:
	Cleaned and processed CSV files stored in:
			Output/Pharmatender_Format/
			Output/Chile_Combined_Format/
			Output/Combinación_Vertical/

How It Works:
	Data Download:
			Reads URLs from Control_File.xlsx and downloads files dynamically.
			Extracts ZIP files and saves contents in the temp and Input/ directories.
	Data Validation and Cleaning:
			Verifies column names and sheets in input files.
			Removes whitespace and unwanted characters from text data.
			Converts date columns into Year and Month for analysis.
	Integration with part_2 and part_3:
			Leverages fuzzy matching and entity mapping functions for advanced processing.


File Name: part_2.py

Implements fuzzy matching to identify pharmaceutical products.
Ensures data integrity by filtering out trivial matches.

Primary Functions:
		fuzzy_match_filtering: Matches products using fuzzywuzzy and custom scoring functions.
		extract_dosage: Extracts dosage information from product descriptions.
		select_highest_score: Determines the best match for each product based on scores.

How It Works
	Input Data:
		Requires a preloaded DataFrame (dataframe) containing product descriptions.
		Utilizes a market basket file (Control/New_Market_Basket.xlsx) that defines valid Pactivo and brand names for matching.
	Fuzzy Matching:
		Pactivo:
			Matches descriptions to Roche Specific Molecules using fuzzy logic.
			Prioritizes exact molecule matches through a custom token set ratio.
		Brands:
			Matches descriptions to brand names using predefined mappings and fuzzy scores.
		Mapping:
			Maps Roche and brands to their corresponding standardized terms from the market basket.
	Dosage Extraction:
		Extracts dosage information (e.g., 10mg/ml, 50UI) from descriptions using regex patterns.
		Removes duplicates and standardizes dosage formats for consistency.
	Final Selection:
		Selects the highest confidence matches for Pactivo and brands.
		Combines EspecificacionComprador and EspecificacionProveedor to produce a final, enriched dataset.


File Name: part_3.py
Handles entity mapping and merging.

Primary Functions:
sucursal_matching: Matches branches (sucursal) to corporate entities using fuzzy logic.
check_and_replace: Standardizes institution names based on custom rules.
create_final_dataframe: Outputs structured datasets for different use cases.
Integrates multiple mapping files for a unified view of pharmaceutical data.


How It Works
	Input Data:
		Requires a processed DataFrame (dataframe) with supplier, buyer, and product details.
		Utilizes several Excel-based mapping files from the Control/ directory:
		Proveedor_Mapping.xlsx: Maps supplier names to corporate entities.
		New_Market_Basket.xlsx: Provides product details for mapping.
		OrganismoPublico_Mapping.xlsx: Maps buyer organizations to standardized names.
		Sector.xlsx: Maps sectors to standardized institutions.
		Mercado_Publico_Mapping.xlsx: Contains supplier and buyer mappings for public markets.
	Supplier Matching:
		Uses fuzzywuzzy for matching supplier names to predefined corporate entities.
		Replaces unmatched or low-confidence names with fallback values.
	Field Mapping:
		Maps supplier (Sucursal_Proveedor), buyer (Razon_Social_Cliente), and product (Pactivo) details using provided mapping files.
		Adds new fields for market or therapeutic areas (Market_or_TA).	
	Language Localization:
		Converts English month names (e.g., "January") to Spanish equivalents (e.g., "Enero").
	Data Transformation:
		Cleans and formats fields such as quantities (cantidad) and prices (precioNeto, totalLineaNeto) into a standardized, readable format.
		Normalizes text fields into title cases where applicable.
Output:
	Returns three enriched DataFrames:
		Pharmatender_final_df: Comprehensive dataset with all matched and mapped details.
		chile_combined_df: Dataset focused on Chile-specific mapping and integration.
		final_df_merged_desintaria: Dataset highlighting institutional recipients and regional summaries.

