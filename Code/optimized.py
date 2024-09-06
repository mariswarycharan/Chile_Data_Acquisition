import pandas as pd
import re
from fuzzywuzzy import fuzz
from fuzzywuzzy import process
import warnings 
warnings.filterwarnings("ignore")

# import ray
# ray.init()

from tqdm import tqdm 
tqdm.pandas() 

import time
start_time = time.time()

# Load Excel and CSV files
market_basket_df = pd.read_excel(r'Control\Market_Basket.xlsx')
market_basket_df['Pactivo_Copy'] = market_basket_df['Pactivo']
market_basket_df['Brand_Generic_Biosimilar_Names_Copy'] = market_basket_df['Brand/Generic/Biosimilar Names']

bulk_downloads_file = pd.read_csv(r"D:\Downloads\April_Bulk.csv")

# Filtering and extraction (Vectorized)
filtered_data_pactivo = market_basket_df[(market_basket_df['Match with pactivo'] == 'Y') & (market_basket_df['Keep (Y/N)'] == 'Y')]
pactivo = filtered_data_pactivo['Pactivo_Copy'].str.lower().unique()

filtered_data_brand = market_basket_df[(market_basket_df['Match with Brand'] == 'Y') & (market_basket_df['Keep (Y/N)'] == 'Y')]
brand = filtered_data_brand['Brand_Generic_Biosimilar_Names_Copy'].str.lower().unique()

# Preprocess function to clean the strings
def preprocess_string(product_string):
    return re.sub(r'[\/,+\-._]', ' ', product_string)

# Vectorized function for molecule extraction
def extract_molecules(product_string):
    known_molecules = [
        ["trastuzumab", "emtansina"], ["pertuzumab", "trastuzumab"],
        ["peginterferon", "b", "1a"], ["interferon", "b", "1a"], 
        ["factor", "antihemofilico", "vii"], ["factor", "antihemofilico", "viii"],
        ["factor recombinante"], ["factor 7"], ["factor von willebrand"], 
        ["columvi"], ["lucentis"]
    ]
    
    molecules_in_string = [mol_list for mol_list in known_molecules 
                           if all(mol in product_string.lower() for mol in mol_list)]
    
    if len(molecules_in_string) >= 2:
        return molecules_in_string[1]
    return molecules_in_string

# Custom token set ratio
def custom_token_set_ratio(str1, str2):
    str1 = preprocess_string(str1).lower()
    str2 = preprocess_string(str2).lower()

    molecules_str1 = extract_molecules(str1)
    molecules_str2 = extract_molecules(str2)

    if molecules_str1 == molecules_str2:
        return 100

    return fuzz.token_set_ratio(str1, str2)

# Matching functions with vectorization
def find_matches_vectorized(df, choices, score_cutoff=81):
    trivial_strings = set(["", ".", ",", ":", ";", "-", "_", "/", "\\", "|", "?", "!", "*", "&", "%", "$", "#", "@", "^", "(", ")", "[", "]", "{", "}", "<", ">", "''", '""', "``", "~", "`", "+", "=", " "])

    def get_best_match(espec):
        if espec not in trivial_strings and espec.strip():
            match = process.extractOne(preprocess_string(espec), choices, scorer=custom_token_set_ratio, score_cutoff=score_cutoff)
            if match:
                return match[0], match[1]
        return '', 0

    comprador_results = df['EspecificacionComprador'].fillna('').map(get_best_match)
    proveedor_results = df['EspecificacionProveedor'].fillna('').map(get_best_match)

    return pd.DataFrame({
        'Best Match Comprador': comprador_results.map(lambda x: x[0]),
        'Match Score Comprador': comprador_results.map(lambda x: x[1]),
        'Best Match Proveedor': proveedor_results.map(lambda x: x[0]),
        'Match Score Proveedor': proveedor_results.map(lambda x: x[1]),
    })

# apply matching functions using vectorized approach
bulk_downloads_file = bulk_downloads_file.join(find_matches_vectorized(bulk_downloads_file, pactivo).add_prefix('Pactivo_'))
bulk_downloads_file = bulk_downloads_file.join(find_matches_vectorized(bulk_downloads_file, brand).add_prefix('Brand_'))

# Mapping function for pactivo from brand
pactivo_brand_map = dict(zip(market_basket_df['Brand_Generic_Biosimilar_Names_Copy'].str.lower(), market_basket_df['Pactivo_Copy']))

bulk_downloads_file['Mapped Pactivo from Comprador Brand'] = bulk_downloads_file['Brand_Best Match Comprador'].map(pactivo_brand_map).fillna('')
bulk_downloads_file['Mapped Pactivo from Proveedor Brand'] = bulk_downloads_file['Brand_Best Match Proveedor'].map(pactivo_brand_map).fillna('')

# Insert mapped pactivo
bulk_downloads_file.insert(loc=bulk_downloads_file.columns.get_loc("Brand_Best Match Comprador") + 1, column='Mapped Pactivo from Comprador Brand', value=bulk_downloads_file.pop('Mapped Pactivo from Comprador Brand'))
bulk_downloads_file.insert(loc=bulk_downloads_file.columns.get_loc("Brand_Best Match Proveedor") + 1, column='Mapped Pactivo from Proveedor Brand', value=bulk_downloads_file.pop('Mapped Pactivo from Proveedor Brand'))

# Selecting the highest score (Vectorized)
def select_highest_score_vectorized(row):
    scores = {
        row['Pactivo_Best Match Comprador']: row['Pactivo_Match Score Comprador'],
        row['Pactivo_Best Match Proveedor']: row['Pactivo_Match Score Proveedor'],
        row['Mapped Pactivo from Comprador Brand']: row['Brand_Match Score Comprador'],
        row['Mapped Pactivo from Proveedor Brand']: row['Brand_Match Score Proveedor'],
    }

    highest_match = max(scores, key=lambda k: scores[k] if k else 0)
    return pd.Series([highest_match, scores[highest_match]])

bulk_downloads_file[['Pactivo_Final', 'Pactivo_Highest_Score']] = bulk_downloads_file.apply(select_highest_score_vectorized, axis=1)

# Selecting final brand (Vectorized)
def select_final_brand_vectorized(row):
    if row['Brand_Match Score Comprador'] > row['Brand_Match Score Proveedor']:
        return pd.Series([row['Brand_Best Match Comprador'], row['Brand_Match Score Comprador']])
    return pd.Series([row['Brand_Best Match Proveedor'], row['Brand_Match Score Proveedor']])

bulk_downloads_file[['Brand_Final', 'Brand_Highest_Score']] = bulk_downloads_file.apply(select_final_brand_vectorized, axis=1)

# Mapping 'Pactivo_Final' to 'Rename Pactivo'
pactivo_rename_mapping = dict(zip(market_basket_df['Pactivo'].str.lower(), market_basket_df['Rename Pactivo'].str.lower()))
bulk_downloads_file['Pactivo_Renamed'] = bulk_downloads_file['Pactivo_Final'].map(pactivo_rename_mapping).fillna(bulk_downloads_file['Pactivo_Final'])

# Dosage extraction with vectorization
def extract_dosage(text):
    if isinstance(text, str):
        text = text.replace(',', '.')
        matches = re.findall(r'(\b\d+(\.\d+)?\s*(%|MG/ML|mg|ml|UI)\b)', text, re.IGNORECASE)
        return ', '.join(match[0].strip() for match in matches) if matches else 'Not Found'
    return 'Not Found'

bulk_downloads_file['Dosage_comprador'] = bulk_downloads_file['EspecificacionComprador'].fillna('').map(extract_dosage)
bulk_downloads_file['Dosage_proveedor'] = bulk_downloads_file['EspecificacionProveedor'].fillna('').map(extract_dosage)

# Selecting final dosage
bulk_downloads_file['Dosage_Final'] = bulk_downloads_file['Dosage_comprador'].where(bulk_downloads_file['Dosage_comprador'] != 'Not Found', bulk_downloads_file['Dosage_proveedor'])

bulk_downloads_file['Dosage_pharma_comprador'] = bulk_downloads_file['Dosage_comprador']
bulk_downloads_file['Dosage_pharma_proveedor'] = bulk_downloads_file['Dosage_proveedor']

# Selecting pharma dosage
bulk_downloads_file['Dosage_pharma'] = bulk_downloads_file['Dosage_pharma_comprador'].where(bulk_downloads_file['Dosage_pharma_comprador'] != 'Not Found', bulk_downloads_file['Dosage_pharma_proveedor'])

# Save the final result
bulk_downloads_file.to_csv('April_Bulk_Presentacion.csv', index=False, encoding='utf-8-sig')


end_time = time.time()
print(f"Time taken: {end_time - start_time} seconds")