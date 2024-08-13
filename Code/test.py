import pandas as pd
import logging

# Configure logging
logging.basicConfig(filename='processing_log.log', level=logging.INFO, 
                    format='%(asctime)s - %(levelname)s - %(message)s')

# Define the file paths
file_path = r'D:\Roche\Chili_Data_Acquisition\temp\2024-1.csv'
final_file_path = 'Output_2024_5.csv'

try:
    # Load the CSV file using the specified delimiter and encoding
    processed_df = pd.read_csv(file_path, delimiter=';', encoding='latin1')
    logging.info(f"File read successfully. Data shape: {processed_df.shape}")
    logging.info(f"Head of data after loading:\n{processed_df.head()}")
except Exception as e:
    logging.error(f"Error reading the file: {e}")
    processed_df = pd.DataFrame()  # Create an empty DataFrame if there's an error

# Check if the DataFrame is not empty
if not processed_df.empty:
    # Replace commas with periods in specific columns and convert them to float
    for column in ['totalLineaNeto', 'precioNeto', 'cantidad']:
        processed_df[column] = processed_df[column].astype(str).str.replace(',', '.').astype(float)
    
    logging.info(f"Data shape after replacing commas and converting to float: {processed_df.shape}")
    logging.info(f"Head of data after replacing commas and converting to float:\n{processed_df.head()}")

    # Function to clean specific columns
    def clean_column(column):
        if column.dtype == 'object':
            return column.str.replace(r'\s+', ' ', regex=True)\
                         .str.replace(r'\t|\r\n|\r|\n', '', regex=True)\
                         .str.strip()
        return column

    # Apply the cleaning function to all columns except specified ones
    columns_to_clean = [col for col in processed_df.columns if col not in [
        'cantidad', 'Descuentos', 'totalCargos', 'totalDescuentos',
        'FechaCreacion', 'FechaEnvio', 'FechaSolicitudCancelacion', 
        'fechaUltimaModificacion', 'FechaAceptacion', 'FechaCancelacion'
    ]]
    for col in columns_to_clean:
        if processed_df[col].dtype == 'object':
            processed_df[col] = clean_column(processed_df[col])

    logging.info(f"Data shape after cleaning: {processed_df.shape}")
    logging.info(f"Head of data after cleaning:\n{processed_df.head()}")

    # Converting 'FechaEnvio' column to datetime format if it's not already
    processed_df['FechaEnvio'] = pd.to_datetime(processed_df['FechaEnvio'], errors='coerce')

    # Extracting year and converting month to words
    processed_df['Year'] = processed_df['FechaEnvio'].dt.year
    processed_df['Month'] = processed_df['FechaEnvio'].dt.strftime('%B')

    logging.info(f"Data shape after extracting year and converting months: {processed_df.shape}")
    logging.info(f"Head of data after extracting year and converting months:\n{processed_df[['FechaEnvio', 'Year', 'Month']].head()}")

    # Save the cleaned and updated data to a new CSV file with utf-8-sig encoding
    try:
        processed_df.to_csv(final_file_path, index=False, encoding='utf-8-sig')
        logging.info(f'Processed and cleaned data saved to {final_file_path}')
    except Exception as e:
        logging.error(f"Error writing the file: {e}")
else:
    logging.warning("No data to save.")