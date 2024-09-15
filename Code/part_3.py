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
    
    Organismo_Publico_mapping_file_path = 'Control/OrganismoPublico_Mapping.xlsx'
    # Read the OrganismoPublico_Mapping and the main data file
    organismo_mapping_df = pd.read_excel(Organismo_Publico_mapping_file_path)

    # Perform the mapping by merging both dataframes on the 'OrganismoPublico' column
    merged_df = pd.merge(final_filtered_data, organismo_mapping_df, on='OrganismoPublico', how='left')

    # Replace the 'OrganismoPublico' column with the 'Mapped_Razon_Social_Cliente' column where available
    merged_df['OrganismoPublico'] = merged_df['Mapped_Razon_Social_Cliente'].combine_first(merged_df['OrganismoPublico'])

    # Drop the 'Mapped_Razon_Social_Cliente' column as it's no longer needed
    final_df = merged_df.drop(columns=['Mapped_Razon_Social_Cliente'])

    # Dictionary mapping specific values from "UnidadCompra" to replace in "OrganismoPublico"
    replacement_dict = {
        'COMANDO DE APOYO A LA FUERZA': 'Replacement Value 1',
        'DIRECCION DE ABASTECIMIENTO DE LA ARMADA': 'Replacement Value 2',
        'SERVICIO DE SALUD CHILOE': 'Replacement Value 3',
        'FUERZA AEREA DE CHILE COMANDO LOGISTICO': 'Replacement Value 4'
    }

    # Replacing values in "OrganismoPublico" based only on the specific "UnidadCompra" values in the dictionary
    final_df['OrganismoPublico'] = final_df.apply(
        lambda row: replacement_dict.get(row['UnidadCompra'], row['OrganismoPublico']),
        axis=1
    )


    sector_df = pd.read_excel('Control/Sector.xlsx')

    # Create a mapping dictionary from the Excel file
    mapping_dict = sector_df.set_index('Sector')['Maped_Instituciones'].to_dict()
    # Map the 'sector' column to the 'Maped_Instituciones' values
    final_df['Instituciones'] = final_df['sector'].map(mapping_dict)

    target_phrase = 'Cenabast'

    # Replacement logic based on the presence of the specific phrase
    final_df['Instituciones'] = final_df.apply(
        lambda row: 'Cenabast' if target_phrase in row['OrganismoPublico'] else row['Instituciones'],
        axis=1)
    final_df['cantidad'] = final_df['cantidad'].astype(int)
    final_df['precioNeto'] = final_df['precioNeto'].astype(int)
    final_df['totalLineaNeto'] = final_df['totalLineaNeto'].astype(int)

    # Format the columns with a comma separator and 0 decimal places
    final_df['cantidad'] = final_df['cantidad'].apply(lambda x: "{:,}".format(x))
    final_df['precioNeto'] = final_df['precioNeto'].apply(lambda x: "{:,}".format(x))
    final_df['totalLineaNeto'] = final_df['totalLineaNeto'].apply(lambda x: "{:,}".format(x))
    
    
    final_df = final_df[["Codigo", "Link", "EspecificacionComprador", "EspecificacionProveedor", "OrganismoPublico", "Sucursal_Proveedor", "Pactivo", "Brand", "Presentación", "cantidad", "precioNeto", "totalLineaNeto", "FechaEnvio", "Mes", "Month", "Year", "Market_or_TA", "RutUnidadCompra", "CiudadUnidadCompra", "RutSucursal", "sector","Instituciones", "RegionUnidadCompra", "Tipo", "CodigoLicitacion", "CorporacionesPHT"]
]
    
    return final_df

