import requests
import zipfile
import io
import os
import logging
import sys
import concurrent.futures
from datetime import datetime
from tqdm import tqdm
import pandas as pd
import shutil
import boto3
from botocore.exceptions import NoCredentialsError

# Set up logging
logging.basicConfig(filename='process.log', level=logging.INFO, 
                    format='%(asctime)s - %(levelname)s - %(message)s')

def verify_columns(df, required_columns):
    """ Verify if the required columns are present in the DataFrame """
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        logging.error(f"Missing columns: {missing_columns}. Actual columns: {df.columns}")
        return False
    logging.info("All required columns are present")
    return True

def download_data():
    try:
        # Read control files and verify sheets
        required_sheets = ["Link", "Month"]
        xls = pd.ExcelFile(r"Control/Control_File.xlsx")
        if set(required_sheets) <= set(xls.sheet_names):
            Controll_file_Link = pd.read_excel(xls, sheet_name="Link")
            Controll_file_Month = pd.read_excel(xls, sheet_name="Month")

            if not (verify_columns(Controll_file_Link, ['Downloadable_Link']) and
                    verify_columns(Controll_file_Month, ['Month'])):
                sys.exit("Column verification failed")

            URL_starting = Controll_file_Link['Downloadable_Link'].iloc[0]
            Month_list = Controll_file_Month['Month'].tolist()

        else:
            logging.error(f"Missing sheet: {xls.sheet_names}. Required: {required_sheets}")
            sys.exit("Sheet verification failed")

        if os.path.exists("temp"):
            shutil.rmtree("temp")
        os.makedirs("temp", exist_ok=True)

        for year_month in Month_list:
            url = URL_starting + year_month + ".zip"
            logging.info(f"Downloading data from {url}")

            # Download and extract ZIP file
            response = requests.get(url, stream=True)
            total_size_in_bytes = int(response.headers.get('content-length', 0))
            progress_bar = tqdm(total=total_size_in_bytes, unit='iB', unit_scale=True)

            file_buffer = io.BytesIO()
            for data in response.iter_content(1024):
                progress_bar.update(len(data))
                file_buffer.write(data)
            progress_bar.close()

            if total_size_in_bytes and progress_bar.n != total_size_in_bytes:
                logging.error("Error: Download incomplete")
            else:
                logging.info(f"Downloaded {total_size_in_bytes} bytes successfully")

            # Extract the downloaded ZIP file
            file_buffer.seek(0)
            with zipfile.ZipFile(file_buffer, 'r') as zip_ref:
                zip_ref.extractall(path="temp")
            logging.info(f"Files extracted to temp")

    except Exception as e:
        logging.error(f"Error in download_data: {e}")

def save_csv_to_s3(file_path, bucket_name, s3_file_name):
    s3 = boto3.client('s3')
    try:
        s3.upload_file(file_path, bucket_name, s3_file_name)
        logging.info(f"File {file_path} successfully uploaded to {bucket_name}/{s3_file_name}")
        return True
    except FileNotFoundError:
        logging.error(f"The file {file_path} was not found")
        return False
    except NoCredentialsError:
        logging.error("Credentials not available")
        return False

def process_csv(csv_path):
    initial_output_path = "Output/"
    try:
        processed_df = pd.read_csv(os.path.join('temp', csv_path), delimiter=';', encoding='latin1', low_memory=False)
        logging.info(f"File {csv_path} read successfully.")
    except Exception as e:
        logging.error(f"Error reading the file {csv_path}: {e}")
        return

    if not processed_df.empty:
        try:
            # Define the columns that do not need to be cleaned
            columns_to_clean = [col for col in processed_df.columns if col not in [
                'cantidad', 'Descuentos', 'totalCargos', 'totalDescuentos',
                'FechaCreacion', 'FechaEnvio', 'FechaSolicitudCancelacion', 
                'fechaUltimaModificacion', 'FechaAceptacion', 'FechaCancelacion'
            ]]

            # Clean only the necessary columns
            for col in columns_to_clean:
                if processed_df[col].dtype == 'object':
                    processed_df[col] = processed_df[col].str.replace(r'\s+', ' ', regex=True)\
                                                         .str.replace(r'\t|\r\n|\r|\n', '', regex=True)\
                                                         .str.strip()

            for column in ['totalLineaNeto', 'precioNeto', 'cantidad']:
                if column in processed_df.columns:
                    processed_df[column] = processed_df[column].astype(str).str.replace(',', '.').astype(float)

            processed_df['FechaEnvio'] = pd.to_datetime(processed_df['FechaEnvio'], errors='coerce')
            processed_df['Year'] = processed_df['FechaEnvio'].dt.year
            processed_df['Month'] = processed_df['FechaEnvio'].dt.strftime('%B')

            formatted_time = datetime.now().strftime("%Y.%m.%d__%H.%M.%S")
            final_file_path = os.path.join(initial_output_path, f"{formatted_time}__{os.path.basename(csv_path)}.csv")
            processed_df.to_csv(final_file_path, index=False, encoding='utf-8-sig')
            logging.info(f'Processed and cleaned data saved to {final_file_path}')
        except Exception as e:
            logging.error(f"Error processing {csv_path}: {e}")
    else:
        logging.warning(f"No data to save for {csv_path}.")

def Cleaning_Data():
    csv_files = [f for f in os.listdir("temp") if f.endswith('.csv')]
    with tqdm(total=len(csv_files), desc="Processing files") as progress_bar:
        with concurrent.futures.ThreadPoolExecutor() as executor:
            for csv_path in csv_files:
                executor.submit(process_csv, csv_path)
                progress_bar.update(1)

# Main execution
# download_data()
Cleaning_Data()
