import re
from fuzzywuzzy import fuzz
from fuzzywuzzy import process
import pandas as pd
import ray

ray.init()

# Preprocess function to clean the strings
def preprocess_string(product_string):
    return re.sub(r'[\/,+\-]', ' ', product_string).lower()

# Function to extract molecules from the string, checking for known molecule combinations and exact matches
def extract_molecules(product_string):
    known_molecules = [
        ["trastuzumab", "emtansina"], ["trastuzumab", "emtansine"], ["trastuzumab", "emtancina"],
        ["pertuzumab", "trastuzumab"], ["interferon", "b", "1a"], ["peginterferon", "b", "1a"],
        ["factor", "antihemofilico", "vii"], ["factor", "antihemofilico", "viii"], ["factor", "7"], 
        ["factor", "7a"], ["factor", "recombinante"], ["columvi"], ["lucentis"]
    ]
    
    product_string = preprocess_string(product_string)
    molecules_in_string = [tuple(molecule_list) for molecule_list in known_molecules
                           if all(molecule in product_string for molecule in molecule_list)]
    
    if len(molecules_in_string) >= 2:
        return [molecules_in_string[1]]  # Return only the second match
    return molecules_in_string

# Custom token set ratio that prioritizes exact molecule matches
def custom_token_set_ratio(str1, str2):
    molecules_str1 = extract_molecules(str1)
    molecules_str2 = extract_molecules(str2)

    if molecules_str1 and molecules_str2 and molecules_str1 == molecules_str2:
        return 100  # Return max score for exact matches
    
    if set(molecules_str1) == set(molecules_str2):
        return fuzz.token_set_ratio(preprocess_string(str1), preprocess_string(str2))
    
    return 0

@ray.remote
def find_matches_parallel(chunk, pactivo, brand, score_cutoff=81):
    chunk['Best Match Comprador Pactivo'] = chunk['EspecificacionComprador'].apply(
        lambda x: process.extractOne(preprocess_string(x), pactivo, scorer=custom_token_set_ratio, score_cutoff=score_cutoff))
    
    chunk['Best Match Proveedor Pactivo'] = chunk['EspecificacionProveedor'].apply(
        lambda x: process.extractOne(preprocess_string(x), pactivo, scorer=custom_token_set_ratio, score_cutoff=score_cutoff))
    
    chunk['Best Match Comprador Brand'] = chunk['EspecificacionComprador'].apply(
        lambda x: process.extractOne(preprocess_string(x), brand, scorer=custom_token_set_ratio, score_cutoff=score_cutoff))
    
    chunk['Best Match Proveedor Brand'] = chunk['EspecificacionProveedor'].apply(
        lambda x: process.extractOne(preprocess_string(x), brand, scorer=custom_token_set_ratio, score_cutoff=score_cutoff))
    
    return chunk

# Split the dataframe into smaller chunks for parallel processing
def split_dataframe(df, chunk_size):
    return [df[i:i + chunk_size] for i in range(0, len(df), chunk_size)]

# Load Excel and CSV files
market_basket_df = pd.read_excel("Control/Market_Basket.xlsx")
bulk_downloads_file = pd.read_csv("Output/2024.08.16_15.40.02_2024-1.csv", low_memory=False)

# Preprocess columns
market_basket_df['Pactivo_Copy'] = market_basket_df['Pactivo'].str.lower()
market_basket_df['Brand_Generic_Biosimilar_Names_Copy'] = market_basket_df['Brand/Generic/Biosimilar Names'].str.lower()

bulk_downloads_file['EspecificacionComprador'] = bulk_downloads_file['EspecificacionComprador'].astype(str)
bulk_downloads_file['EspecificacionProveedor'] = bulk_downloads_file['EspecificacionProveedor'].astype(str)

# Extract pactivo and brand lists
pactivo = market_basket_df.loc[(market_basket_df['Match with pactivo'] == 'Y') & (market_basket_df['Keep (Y/N)'] == 'Y'), 'Pactivo_Copy'].unique()
brand = market_basket_df.loc[(market_basket_df['Match with Brand'] == 'Y') & (market_basket_df['Keep (Y/N)'] == 'Y'), 'Brand_Generic_Biosimilar_Names_Copy'].unique()

# Process in parallel
chunk_size = 10000
chunks = split_dataframe(bulk_downloads_file, chunk_size)
results = ray.get([find_matches_parallel.remote(chunk, pactivo, brand) for chunk in chunks])
bulk_downloads_file = pd.concat(results)

# Mapping results using vectorized operations
mapping_dict = pd.Series(market_basket_df['Pactivo_Copy'].values, index=market_basket_df['Brand_Generic_Biosimilar_Names_Copy'].values).to_dict()

bulk_downloads_file['Mapped Pactivo from Comprador Brand'] = bulk_downloads_file['Best Match Comprador Brand'].apply(lambda x: x[0] if x else None).map(mapping_dict)
bulk_downloads_file['Mapped Pactivo from Proveedor Brand'] = bulk_downloads_file['Best Match Proveedor Brand'].apply(lambda x: x[0] if x else None).map(mapping_dict)

# Move the columns to their new positions
bulk_downloads_file.insert(loc=bulk_downloads_file.columns.get_loc("Best Match Comprador Brand") + 1, column='Mapped Pactivo from Comprador Brand', value=bulk_downloads_file.pop('Mapped Pactivo from Comprador Brand'))
bulk_downloads_file.insert(loc=bulk_downloads_file.columns.get_loc("Best Match Proveedor Brand") + 1, column='Mapped Pactivo from Proveedor Brand', value=bulk_downloads_file.pop('Mapped Pactivo from Proveedor Brand'))

# Select the highest score
def select_highest_score(row):
    best_match_cols = ['Match Score Comprador Pactivo', 'Match Score Proveedor Pactivo']
    best_score_col = max(best_match_cols, key=lambda x: row[x])
    return row[best_score_col], row[best_score_col]

bulk_downloads_file[['Pactivo_Final', 'Pactivo_Highest_Score']] = bulk_downloads_file.apply(select_highest_score, axis=1)

# Filter out rows with a score of 0
bulk_downloads_file = bulk_downloads_file[bulk_downloads_file['Pactivo_Highest_Score'] > 0]

# Final selection of brand
def select_final_brand(row):
    if row['Match Score Comprador Brand'] >= row['Match Score Proveedor Brand']:
        return row['Best Match Comprador Brand'], row['Match Score Comprador Brand']
    return row['Best Match Proveedor Brand'], row['Match Score Proveedor Brand']

bulk_downloads_file[['Brand_Final', 'Brand_Highest_Score']] = bulk_downloads_file.apply(select_final_brand, axis=1)

# Mapping 'Pactivo_Final' to 'Rename Pactivo'
pactivo_rename_mapping = dict(zip(market_basket_df['Pactivo'].str.lower(), market_basket_df['Rename Pactivo'].str.lower()))
bulk_downloads_file['Pactivo_Renamed'] = bulk_downloads_file['Pactivo_Final'].map(pactivo_rename_mapping).fillna(bulk_downloads_file['Pactivo_Final'])

# Save the results to a CSV file
bulk_downloads_file.to_csv('April_Bulk.csv', index=False, encoding="utf-8-sig")
