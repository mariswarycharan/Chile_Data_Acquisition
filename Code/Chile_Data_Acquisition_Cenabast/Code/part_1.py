import pandas as pd
import re
from fuzzywuzzy import fuzz
from fuzzywuzzy import process
import logging
import os


import warnings 
warnings.filterwarnings("ignore")

from tqdm import tqdm 
tqdm.pandas() 

# import ray
# ray.init()

import time

start_time = time.time()

def fuzzy_match_filtering(dataframe,ini_forget_time):

    if not os.path.exists('log'):
        os.makedirs('log')
    
    logging.basicConfig(filename='log/' + ini_forget_time + '_process.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')
    
    # Load Excel files
    market_basket_df = pd.read_excel("Control/New_Market_Basket.xlsx")
    market_basket_df['Pactivo_Copy'] = market_basket_df['Pactivo']
    market_basket_df['Brand_Generic_Biosimilar_Names_Copy'] = market_basket_df['Brand/Generic/Biosimilar Names']

    bulk_downloads_file = dataframe

    # Filtering and extraction
    filtered_data_pactivo = market_basket_df[(market_basket_df['Match with pactivo'] == 'Y') & (market_basket_df['Keep (Y/N)'] == 'Y')]
    pactivo = filtered_data_pactivo['Pactivo_Copy'].str.lower().unique()

    filtered_data_brand = market_basket_df[(market_basket_df['Match with Brand'] == 'Y') & (market_basket_df['Keep (Y/N)'] == 'Y')]
    brand = filtered_data_brand['Brand_Generic_Biosimilar_Names_Copy'].str.lower().unique()

    # Preprocess function to clean the strings
    def preprocess_string(product_string):
        # Replaces slashes, commas, pluses, hyphens, periods, and underscores with a space
        return re.sub(r'[\/,+\-._]', ' ', product_string)

    # Function to extract molecules from the string, checking for known molecule combinations and exact matches
    def extract_molecules(product_string):
        known_molecules = [
            ["Trastuzumab", "Emtansina"], ["Trastuzumab", "EMTANSINE"], ["Trastuzumab", "Emtancina"],["TRASTUZUMAB","EMTANZSINA"], ["Trastuzumab", "etamsine"],
            ["Pertuzumab", "Trastuzumab"], ["TRASTUZUMAB", "EMTASINA"],["Peginterferon", "B" , "1A"],["Interferon", "B", "1A"],["INTERFERON", "BETA", "1A", "PEGILADO"],["INTERFERON", "BETA", "1A"],["INTERF B1A"],
            ["Factor", "Antihemofilico", "VII"], ["Factor", "Antihemofilico", "VIII"], ["Factor VII"], ["Factor VII A"],
            ["Factor Recombinante"], ["Factor 7"], ["Factor Antihemofilico 7"], ["FACTOR VIII"],
            ["Complejo Anti-Inhibidor Fac VIII"], ["Factor Antihemofilico PM VIII"], ["Factor von Willebrand"],
            ["Factor Antihem PM VIII"], ["Fact Antihemofilico VIII"], ["MCT FAC ANTIH VIII"],
            ["MCT Fact.Antihem.VIII"], ["Haemate P"], ["WILATE"],["columvi"],["lucentis"], ["hemlibra"],["tascenso"], ["fingolimod hcl"], 
            ["complej anti inh fac viii"],["MCT Fact Antihem VIII"], ["Complejo Anti Inhibidor Fac VIII"], ["mct fact antih viii"],["complej anti-inh fac viii"],["INFIZIMAB"]
        ]

        molecules_in_string = [molecule_list for molecule_list in known_molecules
                            if all(molecule.lower() in product_string.lower() for molecule in molecule_list)]

        # Logic to pick the secondly matched molecule if two or more matches found
        if len(molecules_in_string) >= 2:
            return [molecules_in_string[1]]  # Return only the second match
        return molecules_in_string

    # Custom token set ratio that prioritizes exact molecule matches
    def custom_token_set_ratio(str1, str2):
        str1 = preprocess_string(str1).lower()
        str2 = preprocess_string(str2).lower()

        molecules_str1 = extract_molecules(str1)
        molecules_str2 = extract_molecules(str2)

        if molecules_str1 and molecules_str2 and molecules_str1 == molecules_str2:
            return 100  # Return max score for exact matches

        if set(tuple(molecule_list) for molecule_list in molecules_str1) == set(tuple(molecule_list) for molecule_list in molecules_str2):
            return fuzz.token_set_ratio(str1, str2)

        return 0

    # Matching functions with input validation
    def find_matches(row, choices, score_cutoff=81):
        # Extract and preprocess the strings
        especificacion_comprador = preprocess_string(row['Nombre producto genérico']) if isinstance(row['Nombre producto genérico'], str) else ""
        especificacion_proveedor = preprocess_string(row['Nombre producto comercial']) if isinstance(row['Nombre producto comercial'], str) else ""
        
        # Initialize matches to default values
        match_comprador = ('', 0)
        match_proveedor = ('', 0)
        trivial_strings = ["", "??", ".", ",", ":", ";", "-", "_", "/", "\\", "|", "?", "!", "*", "&", "%", "$", "#", "@", "^", "(", ")", "[", "]", "{", "}", "<", ">", "''", '""', "`", "~", "", "+", "=", " ","??''"]

        # Check if strings are valid (non-empty and not just trivial characters)
        if especificacion_comprador and especificacion_comprador.strip() not in trivial_strings:  # Only process if not empty or trivial
            match_comprador = process.extractOne(especificacion_comprador, choices, scorer=custom_token_set_ratio, score_cutoff=score_cutoff)
            if not match_comprador:
                match_comprador = ('', 0)
        
        if especificacion_proveedor and especificacion_proveedor.strip() not in trivial_strings:  # Only process if not empty or trivial
            match_proveedor = process.extractOne(especificacion_proveedor, choices, scorer=custom_token_set_ratio, score_cutoff=score_cutoff)
            if not match_proveedor:
                match_proveedor = ('', 0)
        
        return pd.Series([match_comprador[0], match_comprador[1], match_proveedor[0], match_proveedor[1]],
                        index=['Best Match Comprador', 'Match Score Comprador', 'Best Match Proveedor', 'Match Score Proveedor'])

    print("Operation running for find_matches for pactivo ...")
    logging.info("Operation running for find_matches for pactivo ...")
    bulk_downloads_file[['Best Match Comprador Pactivo', 'Match Score Comprador Pactivo', 'Best Match Proveedor Pactivo', 'Match Score Proveedor Pactivo']] = \
        bulk_downloads_file.progress_apply(lambda row: find_matches(row, pactivo), axis=1)

    print("Operation running for find_matches for Brand ...")
    logging.info("Operation running for find_matches for Brand ...")
    bulk_downloads_file[['Best Match Comprador Brand', 'Match Score Comprador Brand', 'Best Match Proveedor Brand', 'Match Score Proveedor Brand']] = \
        bulk_downloads_file.progress_apply(lambda row: find_matches(row, brand), axis=1)

    # Mapping functions
    def map_pactivo_from_brand(brand_name):
        if pd.isna(brand_name):
            return ''
        mapping = market_basket_df[market_basket_df['Brand_Generic_Biosimilar_Names_Copy'].str.lower() == brand_name.lower()]
        if not mapping.empty:
            return mapping.iloc[0]['Pactivo_Copy']
        return ''

    print("Operation running for map_pactivo_from_brand for Best Match Comprador Brand ...")
    logging.info("Operation running for map_pactivo_from_brand for Best Match Comprador Brand ...")
    bulk_downloads_file['Mapped Pactivo from Comprador Brand'] = bulk_downloads_file['Best Match Comprador Brand'].progress_apply(map_pactivo_from_brand)
    print("Operation running for map_pactivo_from_brand for Best Match Proveedor Brand ...")
    logging.info("Operation running for map_pactivo_from_brand for Best Match Proveedor Brand ...")
    bulk_downloads_file['Mapped Pactivo from Proveedor Brand'] = bulk_downloads_file['Best Match Proveedor Brand'].progress_apply(map_pactivo_from_brand)

    bulk_downloads_file.insert(loc=bulk_downloads_file.columns.get_loc("Best Match Comprador Brand") + 1, column='Mapped Pactivo from Comprador Brand', value=bulk_downloads_file.pop('Mapped Pactivo from Comprador Brand'))
    bulk_downloads_file.insert(loc=bulk_downloads_file.columns.get_loc("Best Match Proveedor Brand") + 1, column='Mapped Pactivo from Proveedor Brand', value=bulk_downloads_file.pop('Mapped Pactivo from Proveedor Brand'))

    # Final selection functions
    def select_highest_score(row):
        
        scores = {
            row['Best Match Comprador Pactivo']: row['Match Score Comprador Pactivo'],
            row['Best Match Proveedor Pactivo']: row['Match Score Proveedor Pactivo'],
            row['Mapped Pactivo from Comprador Brand']: row['Match Score Comprador Brand'],
            row['Mapped Pactivo from Proveedor Brand']: row['Match Score Proveedor Brand']
        }
        if any(score > 0 for score in scores.values()):
            highest_match = max(scores, key=lambda k: scores[k] if k else 0)
            highest_score = scores[highest_match]
            return pd.Series([highest_match, highest_score], index=['Pactivo_Final', 'Pactivo_Highest_Score'])
        return pd.Series([None, 0], index=['Pactivo_Final', 'Pactivo_Highest_Score'])

    print("Operation running for select_highest_score ...")
    logging.info("Operation running for select_highest_score ...")
    bulk_downloads_file[['Pactivo_Final', 'Pactivo_Highest_Score']] = bulk_downloads_file.progress_apply(select_highest_score, axis=1)

    bulk_downloads_file = bulk_downloads_file[bulk_downloads_file['Pactivo_Highest_Score'] > 0]

    def select_final_brand(row):
        comprador_brand = row['Best Match Comprador Brand']
        comprador_score = row['Match Score Comprador Brand']
        proveedor_brand = row['Best Match Proveedor Brand']
        proveedor_score = row['Match Score Proveedor Brand']

        if comprador_score > proveedor_score:
            return pd.Series([comprador_brand, comprador_score], index=['Brand_Final', 'Brand_Highest_Score'])
        elif proveedor_score > comprador_score:
            return pd.Series([proveedor_brand, proveedor_score], index=['Brand_Final', 'Brand_Highest_Score'])
        else:
            # If both scores are equal, prioritize proveedor brand
            if pd.notna(proveedor_brand) and proveedor_brand:
                return pd.Series([proveedor_brand, proveedor_score], index=['Brand_Final', 'Brand_Highest_Score'])
            elif pd.notna(comprador_brand) and comprador_brand:
                return pd.Series([comprador_brand, comprador_score], index=['Brand_Final', 'Brand_Highest_Score'])
            else:
                return pd.Series(['', 0], index=['Brand_Final', 'Brand_Highest_Score'])

    # progress_swifter.progress_apply the function to create the 'Brand Final' and 'Brand Final Score' columns
    # bulk_downloads_file[['Brand', 'Brand_Highest_Score']] = bulk_downloads_file.progress_apply(select_final_brand, axis=1)
    logging.info("Operation running for select_final_brand ...")
    add_two_column = bulk_downloads_file.progress_apply(select_final_brand, axis=1)
    if add_two_column.empty == False:
        bulk_downloads_file[['Brand', 'Brand_Highest_Score']] = add_two_column
    else:
        bulk_downloads_file[['Brand', 'Brand_Highest_Score']] = pd.Series(['', 0], index=['Brand', 'Brand_Highest_Score'])


    # Mapping 'Pactivo_Final' to 'Rename Pactivo'
    pactivo_rename_mapping = dict(zip(market_basket_df['Pactivo'].str.lower(), market_basket_df['Rename Pactivo'].str.lower()))

    bulk_downloads_file['Pactivo'] = bulk_downloads_file['Pactivo_Final'].str.lower().map(pactivo_rename_mapping).fillna(bulk_downloads_file['Pactivo_Final'])

    # Dosage extraction functions
    def extract_dosage(text):
        if isinstance(text, str):
            text = text.replace(',', '.')

            matches = re.findall(
                r'(\b\d+(\.\d+)?\s*(%|MG/ML|mg|ml|UI)\b|\b\d+(\.\d+)?(%|MG/ML|mg|ml|UI)\b|\b\d+-\d+\s*(%|MG/ML|mg|ml|UI)\b|\b\d+(\.\d+)?\s*(%|MG/ML|mg|ml|UI)(?:/\d+(\.\d+)?\s*(%|MG/ML|mg|ml|UI))\b|\b\d+(\.\d+)?\s*\[\s*(%|MG/ML|mg|ml|UI)\s*\]\s*/\s*\d+(\.\d+)?\s*\[\s*(%|MG/ML|mg|ml|UI)\s*\]\b|\b\d+(\.\d+)?(%|MG/ML|mg|ml|UI)(?:/\d+(\.\d+)?(%|MG/ML|mg|ml|UI))\b|\b\d+(\.\d+)?(%|MG/ML|mg|ml|UI)\d+(\.\d+)?(%|MG/ML|mg|ml|UI)\b|\b\d+(\.\d+)?\s*(%|MG/ML|mg|ml|UI)\d+(\.\d+)?\s*(%|MG/ML|mg|ml|UI)\b)',
                text, re.IGNORECASE
            )

            if matches:
                return ', '.join([match[0].strip() for match in matches if match[0]])

        return ''

    def remove_duplicates(text):
        normalized = [re.sub(r'\s+', '', item.lower()) for item in re.split(r'(\d+\s*[a-zA-Z]+)', text) if item.strip()]

        unique_items = []
        for item in normalized:
            if item not in unique_items:
                unique_items.append(item)

        return ' '.join(unique_items)
    
    def extract_first_mg_ui(input_str):
        # Find the index of 'mg' and 'ui'
    
        input_str = input_str.lower().strip()
        
        mg_index = input_str.find('mg')
        ui_index = input_str.find('ui')

        # If both "mg" and "ui" are found, return the first one
        if mg_index != -1 and ui_index != -1:
            if mg_index < ui_index:
                return input_str.split('mg')[0].strip().replace(' ','') + 'mg'
            else:
                return input_str.split('ui')[0].strip().replace(' ','') + 'ui'
        
        # If only "mg" is found
        elif mg_index != -1:
            return input_str.split('mg')[0].strip().replace(' ','') + 'mg'
        
        # If only "ui" is found
        elif ui_index != -1:
            return input_str.split('ui')[0].strip().replace(' ','') + 'ui'
        
        # If neither is found, return None or some default message
        return ""

    # progress_swifter.progress_apply dosage extraction and removal of duplicates after matching
    print("Operation running for remove_duplicates for ENombre producto genérico ...")
    logging.info("Operation running for remove_duplicates for Nombre producto genérico ...")
    value1 = bulk_downloads_file['Nombre producto genérico'].progress_apply(lambda x: remove_duplicates(extract_dosage(x)))
    print("Operation running for remove_duplicates for Nombre producto comercial ...")
    logging.info("Operation running for remove_duplicates for Nombre producto comercial ...")
    value2 = bulk_downloads_file['Nombre producto comercial'].progress_apply(lambda x: remove_duplicates(extract_dosage(x)))
    bulk_downloads_file['Dosage_comprador'] = value1
    bulk_downloads_file['Dosage_pharmatender_comprador'] = bulk_downloads_file['Dosage_comprador'].map(extract_first_mg_ui)
    bulk_downloads_file['Dosage_proveedor'] = value2
    bulk_downloads_file['Dosage_pharmatender_proveedor'] = bulk_downloads_file['Dosage_proveedor'].map(extract_first_mg_ui)


    bulk_downloads_file['Dosage_Final'] = bulk_downloads_file['Dosage_comprador'].where(bulk_downloads_file['Dosage_comprador'] != '', bulk_downloads_file['Dosage_proveedor'])
    bulk_downloads_file['Presentación'] = bulk_downloads_file['Dosage_Final'].map(extract_first_mg_ui)
    
    
    # Save the final result
    # bulk_downloads_file.to_csv('April_Bulk_Presentacion.csv', index=False, encoding = 'utf-8-sig')

    end_time = time.time()  
    print(f"Time taken: {end_time - start_time} seconds")
    
    return bulk_downloads_file


# df = pd.read_csv(r"log\April_Bulk.csv")
# fuzzy_match_filtering(df)