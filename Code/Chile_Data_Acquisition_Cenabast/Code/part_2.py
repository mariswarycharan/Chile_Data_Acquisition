#Rut Comprador
import pandas as pd

# Function to calculate and format RUT check digit
def calculate_rut_check_digit(rut):
    reversed_digits = list(map(int, str(rut)))[::-1]
    factors = [2, 3, 4, 5, 6, 7]
    sum_products = sum(d * factors[i % 6] for i, d in enumerate(reversed_digits))
    remainder = sum_products % 11
    check_digit = 11 - remainder
    if check_digit == 10:
        check_digit = 'K'
    elif check_digit == 11:
        check_digit = '0'
    else:
        check_digit = str(check_digit)
    formatted_rut = f'{rut:,.0f}'.replace(',', '.') + '-' + check_digit
    return formatted_rut


def Mapping_File_Format(dataframe):

    # Read the Excel file
    #   # Change this to your file path
    data = dataframe

    # Check if the column 'Codigo Solicitante' exists
    if 'Cliente solicitante' in data.columns:
        # Calculate the formatted RUT with check digit for each entry
        data['Rut Comprador Formatted'] = data['Cliente solicitante'].apply(calculate_rut_check_digit)
    else:
        print("Column 'Cliente solicitante' not found in the data.")

    # Comprador Mapping

    # Load the data
    out_with_rut_df = data
    comprador_cenabast_df = pd.read_excel('Control/Cenabast_Mapping.xlsx', sheet_name='Comprador_Cnbt')

    # Merge the dataframes ensuring the original rows of 'Out with Rut Comprador.xlsx' are unchanged
    merged_df = out_with_rut_df.merge(comprador_cenabast_df[['Nombre cliente solicitante', 'Comprador', 'Segmento Comprador']],
                                    on='Nombre cliente solicitante',
                                    how='left')

    # Replace empty 'Comprador' values with 'Nombre cliente solicitante' values
    merged_df['Comprador'] = merged_df['Comprador'].fillna(merged_df['Nombre cliente solicitante'])


    #Proveedor Mapping

    # Load the data
    out_with_rut_comprador = merged_df
    proveedor_cenabast_df = pd.read_excel('Control/Cenabast_Mapping.xlsx', sheet_name='Proveedor_Cnbt')

    # Merge the dataframes ensuring the original rows of 'Out with Rut Comprador.xlsx' are unchanged
    merged_df_2 =out_with_rut_comprador.merge(proveedor_cenabast_df[['Nombre proveedor', 'Rut_Proveedor', 'Proveedor', 'Proveedor Asociado']],
                                    on='Nombre proveedor',
                                    how='left')

    # Replace empty 'Proveedor' values with 'Nombre proveedor' values
    merged_df_2['Proveedor'] = merged_df_2['Proveedor'].fillna(merged_df['Nombre proveedor'])

    # Replace empty 'Proveedor Asociado' values with 'Nombre proveedor' values
    merged_df_2['Proveedor Asociado'] = merged_df_2['Proveedor Asociado'].fillna(merged_df['Nombre proveedor'])

    #Institucion Destinataria

    # Load the data from both Excel files
    merged_df_3 = merged_df_2
    Institucion_Destinataria_mapping_data = pd.read_excel('Control/Cenabast_Mapping.xlsx', sheet_name='Institucion_Destinataria')


    merged_df_3['Nombre cliente destinatario'] = merged_df_3['Nombre cliente destinatario'].str.upper()
    Institucion_Destinataria_mapping_data['Institucion Destinataria'] = Institucion_Destinataria_mapping_data['Institucion Destinataria'].str.upper()

    # Merge the 'data_2024' dataframe with the 'mapping_data' dataframe based on the 'RutUnidadCompra' and 'Rut Comprador' columns
    merged_df_4 = merged_df_3.merge(Institucion_Destinataria_mapping_data, left_on="Nombre cliente destinatario", right_on="Institucion Destinataria", how="left")
    
    from datetime import datetime
    # Convert the "Fecha de entrega" column to a short date format

    merged_df_4['Fecha de entrega'] = pd.to_datetime(merged_df_4['Fecha de entrega']).dt.strftime('%d-%m-%Y')

    # Medida Maping
    final_data_merged = merged_df_4
    mapping_data = pd.read_excel("Control/Cenabast_Mapping.xlsx", sheet_name="Mapping_Medida")

    final_data_merged['Pactivo'] = final_data_merged['Pactivo'].str.upper()
    mapping_data['Pactivo'] = mapping_data['Pactivo'].str.upper()

    # Merge the 'final_data_merged' dataframe with the 'Pactivo' dataframe based on the 'Pactivo' column
    final_data_merged = final_data_merged.merge(mapping_data, left_on="Pactivo", right_on="Pactivo", how="left")

    # Replace the value in UnidadMedida based on Rut_Proveedor condition
    final_data_merged.loc[final_data_merged['Rut_Proveedor'] == '80.621.200-8', 'UnidadMedida'] = 'Comprimido'

    final_data_merged['Pactivo'] = final_data_merged['Pactivo'].str.title()

    # Load the data from both Excel files
    data_with_market_ta = final_data_merged
    mapping_data = pd.read_excel("Control/Cenabast_Mapping.xlsx", sheet_name="Final Market Basket")

    data_with_market_ta['Pactivo'] = data_with_market_ta['Pactivo'].str.upper()
    mapping_data['Pactivo'] = mapping_data['Pactivo'].str.upper()

    # Merge the 'data_with_market_ta' dataframe with the 'mapping_data' dataframe based on the 'RutUnidadCompra' and 'Rut Comprador' columns
    data_with_market_ta_merged = data_with_market_ta.merge(mapping_data, left_on="Pactivo", right_on="Pactivo", how="left")

    columns_to_drop = ['Rename Pactivo', 'Brand/Generic/Biosimilar Names', 'Keep (Y/N)', 'Match with pactivo', 'Match with Brand']

    # Drop the columns
    data_with_market_ta_merged.drop(columns=columns_to_drop, inplace=True)

    #Rename Columns
    data_with_market_ta_merged = data_with_market_ta_merged[["Orden de Compra", "Nombre producto genérico", "Nombre producto comercial","Comprador","Proveedor", "Pactivo", "Brand", "UnidadMedida","Presentación", "Cantidad unitaria", "Precio unitario neto", "Monto Neto", "Fecha de entrega", "Mes", "Año", "Market or TA", "Rut Comprador Formatted", "Comuna", "Rut_Proveedor","Segmento Comprador", "Nombre región", "N°Región", "Proveedor Asociado", "Intitución Destinataria Homologada"]]
    columns_to_rename = {
            "Orden de Compra":"Codigo",
            "Nombre producto genérico":"EspecificacionComprador",
            "Nombre producto comercial":"EspecificacionProveedor",
            "Comprador":"Razon_Social_Cliente",
            "Proveedor":"Sucursal_Proveedor",
            "Cantidad unitaria":"cantidad",
            "Precio unitario neto":"precioNeto",
            "Monto Neto":"totalLineaNeto",
            "Fecha de entrega":"Fecha",
            "Mes":"Month",
            "Año":"Year",
            "Market or TA":"Market_or_TA",
            "Rut Comprador Formatted":"RutUnidadCompra",
            "Rut_Proveedor":"RutSucursal",
            "Segmento Comprador":"Instituciones",
            "Comuna":"CiudadUnidadCompra",
            "Nombre región": "Region",
            "N°Región":"Region_Number",
            "Proveedor Asociado":"CorporacionesPHT"

        }
    
    final_data = data_with_market_ta_merged
    # Rename the columns
    final_data.rename(columns=columns_to_rename, inplace=True)

    #Origin Column
    final_data['Origin'] ='Cnbt'

    # Dictionary to map English months to Spanish
    month_translation = {
            1: 'Enero',
            2: 'Febrero',
            3: 'Marzo',
            4: 'Abril',
            5: 'Mayo',
            6: 'Junio',
            7: 'Julio',
            8: 'Agosto',
            9: 'Septiembre',
            10: 'Octubre',
            11: 'Noviembre',
            12: 'Diciembre'
    }

    # Add the 'mes' column to your DataFrame
    final_data['Mes'] = final_data['Month'].map(month_translation)

    # Dictionary to map Region Numbers
    Region_Number_2 = {
        'I': 1,
        'II': 2,
        'III': 3,
        'IV': 4,
        'V': 5,
        'VI': 6,
        'VII': 7,
        'VIII': 8,
        'IX': 9,
        'X': 10,
        'XI': 11,
        "XII": 12,
        'RM': 13,
        'XIV': 14,
        'XV': 15,
        'XVI': 16,
    }

    final_data['Intitución Destinataria Homologada'] = final_data['Intitución Destinataria Homologada'].fillna(final_data['Razon_Social_Cliente'])


    # Replace 'Region_Number' column values using the dictionary
    final_data['Region_Number'] = final_data['Region_Number'].map(Region_Number_2)

    # Create the Pactivo+CorporacionesPHT column
    final_data['Pactivo+CorporacionesPHT'] = final_data['Pactivo'].astype(str) + '-' + final_data['CorporacionesPHT'].astype(str)

    #Chile Combined
    final_data = final_data[["Codigo", "EspecificacionComprador", "EspecificacionProveedor","Razon_Social_Cliente","Sucursal_Proveedor", "Pactivo", "Brand", "UnidadMedida","Presentación", "cantidad", "precioNeto", "totalLineaNeto", "Fecha", "Mes", "Month", "Year", "Market_or_TA", "RutUnidadCompra", "CiudadUnidadCompra", "RutSucursal","Instituciones", "Region", "Region_Number", "CorporacionesPHT","Pactivo+CorporacionesPHT","Intitución Destinataria Homologada", "Origin"]]
    # Title case
    columns_to_convert = ['Razon_Social_Cliente', 'Sucursal_Proveedor', 'Pactivo', 'Brand','CiudadUnidadCompra','CorporacionesPHT','Pactivo+CorporacionesPHT']
    final_data[columns_to_convert] = final_data[columns_to_convert].apply(lambda x: x.str.title())

    final_data['cantidad'] = final_data['cantidad'].astype(int)
    final_data['precioNeto'] = final_data['precioNeto'].astype(int)
    final_data['totalLineaNeto'] = final_data['totalLineaNeto'].astype(int)

    # Format the columns with a comma separator and 0 decimal places
    final_data['cantidad'] = final_data['cantidad'].apply(lambda x: "{:,}".format(x))
    final_data['precioNeto'] = final_data['precioNeto'].apply(lambda x: "{:,}".format(x))
    final_data['totalLineaNeto'] = final_data['totalLineaNeto'].apply(lambda x: "{:,}".format(x))

    final_Combinacion_Vertical = final_data[["Year", "Month", "Region", "CorporacionesPHT", "Pactivo", "totalLineaNeto", "Market_or_TA", "Mes", "Intitución Destinataria Homologada", "Pactivo+CorporacionesPHT", "Origin","cantidad"]]
    # Save the modified DataFrame to an Excel file

    return final_data , final_Combinacion_Vertical

