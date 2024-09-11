from fuzzywuzzy import process, fuzz
import pandas as pd
import logging

# Load the datasets
from tqdm import tqdm 
tqdm.pandas() 

def sucursal_matching(dataframe,ini_forget_time):
    
    logging.basicConfig(filename='log/' + ini_forget_time + '_process.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')
   
    final_bulk = dataframe
    proveedor_mapping = pd.read_excel('Control/Proveedor Mapping.xlsx')

    # Function to perform fuzzy matching with Token Set Ratio and return original name if the score is below the threshold
    def get_best_match_with_fallback(name, choices, scorer=fuzz.token_set_ratio, limit=1, threshold=90):
        match = process.extractOne(name, choices, scorer=scorer)
        if match and match[1] >= threshold:
            return match
        else:
            return (name, None)  # Return original name if no suitable match found

    # Create a list of choices for fuzzy matching from the Proveedor Mapping
    choices = proveedor_mapping['Sucursal / NombreProveedor'].tolist()

    # Apply fuzzy matching to the 'Sucursal' column with a fallback to the original name if below threshold
    print("Sucursal matching started ...")
    final_bulk['Sucursal Proveedor'], final_bulk['Match Score'] = zip(*final_bulk['Sucursal'].progress_apply(
        lambda x: get_best_match_with_fallback(x, choices, scorer=fuzz.token_set_ratio, threshold=90) if pd.notna(x) else (None, None)
    ))

    # Create a mapping dictionary from Proveedor Mapping data for corporation matching
    corp_mapping = proveedor_mapping.set_index('Sucursal / NombreProveedor')['CorporacionesPHT'].to_dict()

    # Map the "Corporation Match" column using "Sucursal Proveedor" as the key to look up the "CorporacionesPHT"
    final_bulk['Corporation Match'] = final_bulk['Sucursal Proveedor'].map(corp_mapping)

    # Fill blanks in 'Corporation Match' with values from 'NombreProveedor'
    final_bulk['Corporation Match'] = final_bulk['Corporation Match'].fillna(final_bulk['NombreProveedor'])

    final_filtered_data = final_bulk.loc[final_bulk['codigoProductoONU'] != 46171610]
    
    final_filtered_data = final_filtered_data[~(
        final_filtered_data['EspecificacionComprador'].str.contains(r'\(octaplex\)|\boctaplex\b', case=False, na=False) | 
        final_filtered_data['EspecificacionProveedor'].str.contains(r'\(octaplex\)|\boctaplex\b', case=False, na=False)
        )]
    
    return final_filtered_data

