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
    market_basket_df = pd.read_excel("Control/New_Market_Basket.xlsx")

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
    final_bulk['Sucursal_Proveedor'], final_bulk['Match Score'] = zip(*final_bulk['Sucursal'].progress_apply(
        lambda x: get_best_match_with_fallback(x, choices, scorer=fuzz.token_set_ratio, threshold=90) if pd.notna(x) else (None, None)
    ))

    # Create a mapping dictionary from Proveedor Mapping data for corporation matching
    corp_mapping = proveedor_mapping.set_index('Sucursal / NombreProveedor')['CorporacionesPHT'].to_dict()

    # Map the "Corporation Match" column using "Sucursal Proveedor" as the key to look up the "CorporacionesPHT"
    final_bulk['CorporacionesPHT'] = final_bulk['Sucursal_Proveedor'].map(corp_mapping)

    # Fill blanks in 'Corporation Match' with values from 'NombreProveedor'
    final_bulk['CorporacionesPHT'] = final_bulk['CorporacionesPHT'].fillna(final_bulk['Sucursal'])

    final_filtered_data = final_bulk.loc[final_bulk['codigoProductoONU'] != 46171610]
    
    final_filtered_data = final_filtered_data[~(
        final_filtered_data['EspecificacionComprador'].str.contains(r'\(octaplex\)|\boctaplex\b', case=False, na=False) | 
        final_filtered_data['EspecificacionProveedor'].str.contains(r'\(octaplex\)|\boctaplex\b', case=False, na=False)
        )]
    
    # Dictionary to map English months to Spanish
    month_translation = {
        'January': 'Enero',
        'February': 'Febrero',
        'March': 'Marzo',
        'April': 'Abril',
        'May': 'Mayo',
        'June': 'Junio',
        'July': 'Julio',
        'August': 'Agosto',
        'September': 'Septiembre',
        'October': 'Octubre',
        'November': 'Noviembre',
        'December': 'Diciembre'
    }

    # Add the 'mes' column to your DataFrame
    final_filtered_data['Mes'] = final_filtered_data['Month'].map(month_translation)

    # Find the index of the 'Month' column
    month_index = final_filtered_data.columns.get_loc('Month')
    final_filtered_data.insert(month_index + 1, 'Mes', final_filtered_data.pop('Mes'))


    # Convert 'Corporation Match' column to uppercase
    final_filtered_data['CorporacionesPHT'] = final_filtered_data['CorporacionesPHT'].str.upper()
    
    Rename_Pactivo_dict = {j.lower():i for i,j in market_basket_df.drop_duplicates("Rename Pactivo")[['Market or TA','Rename Pactivo']].to_numpy()}
    final_filtered_data['Market_or_TA'] = final_filtered_data['Pactivo'].str.lower().map(Rename_Pactivo_dict)
    
    
    final_filtered_data = final_filtered_data[["Codigo", "Link", "EspecificacionComprador", "EspecificacionProveedor", "OrganismoPublico", "Sucursal_Proveedor", "Pactivo", "Brand", "Presentación", "cantidad", "precioNeto", "totalLineaNeto", "FechaEnvio", "Mes", "Month", "Year", "Market_or_TA", "RutUnidadCompra", "CiudadUnidadCompra", "RutSucursal", "sector", "RegionUnidadCompra", "Tipo", "CodigoLicitacion", "CorporacionesPHT"]
]
    
    return final_filtered_data

