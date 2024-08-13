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
import time

import ray

ray.init()

# Set up logging
logging.basicConfig(filename='log/process.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')
 
def verify_columns(df, required_columns):
    """ Verify if the required columns are present in the DataFrame """
 
    if required_columns != df.columns:
        logging.error(f"Missing columns:  {df.columns} and Actual columns are {required_columns} , by mistake written columns are {df.columns} and please check column names are correct")
        # raise ValueError(f"Missing columns: {missing_columns} and Actual columns are {required_columns} , by mistake written columns are {df.columns}")
        return False
    else:
        logging.info(f"All required columns are present ==> columns are {df.columns}")
        return True
 
def download_data():
    try:
        # Read control files and verify sheets
        required_sheets = ["Link", "Month"]
        xls = pd.ExcelFile(r"Control\Control_File.xlsx")
 
        if required_sheets == xls.sheet_names:
           
            Controll_file_Link = pd.read_excel(xls, sheet_name="Link")
            Controll_file_Month = pd.read_excel(xls, sheet_name="Month")
           
            if verify_columns(Controll_file_Link, ['Downloadable_Link']) and verify_columns(Controll_file_Month, ['Month']):
               
                URL_starting = Controll_file_Link['Downloadable_Link'][0]
                Month_list = list(Controll_file_Month['Month'])
               
            else:
                logging.error(f"Missing columns:  {Controll_file_Link.columns} and {Controll_file_Month.columns} and please check column names are correct")
                sys.exit(f"Missing columns:  {Controll_file_Link.columns} and {Controll_file_Month.columns} and please check column names are correct")
               
        else:
            logging.error(f"Missing sheet: {xls.sheet_names} ==> Actual Sheet names is {required_sheets} and please check sheet names are correct")
            sys.exit(f"Missing sheet: {xls.sheet_names} ==> Actual Sheet names is {required_sheets} and please check sheet names are correct")
 
       
        if  os.path.exists("temp"):
            # Delete the temp folder
            shutil.rmtree("temp")
 
        for year_month in Month_list:
            # Replace year and month in the URL
            url = URL_starting + year_month + ".zip"
            logging.info(f"Downloading data from {url}")
           
            # Send a GET request with stream=True to download the file in chunks
            response = requests.get(url, stream=True)
            total_size_in_bytes = int(response.headers.get('content-length', 0))
            block_size = 1024  # 1 Kilobyte
 
            # Create a progress bar
            progress_bar = tqdm(total=total_size_in_bytes, unit='iB', unit_scale=True)
 
            file_buffer = io.BytesIO()
 
            for data in response.iter_content(block_size):
                progress_bar.update(len(data))
                file_buffer.write(data)
 
            progress_bar.close()
 
            if total_size_in_bytes != 0 and progress_bar.n != total_size_in_bytes:
                logging.error("Error: Something went wrong during the download")
            else:
                logging.info(f"Downloaded {total_size_in_bytes} bytes successfully")
 
            # Extract the downloaded ZIP file
            file_buffer.seek(0)
            formatted_time = datetime.now().strftime("%Y.%m.%d_%H.%M.%S")
            csv_file_path = "Input/" + formatted_time
 
            with zipfile.ZipFile(file_buffer, 'r') as zip_ref:
                zip_ref.extractall(path=csv_file_path)
            logging.info(f"Files extracted to {csv_file_path}")
 
            with zipfile.ZipFile(file_buffer, 'r') as zip_ref:
                zip_ref.extractall(path='temp')
            logging.info("Temporary extraction complete.")
   
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
 
def clean_column(column):
    if column.dtype == 'object':
        return column.str.replace(r'\s+', ' ', regex=True)\
                    .str.replace(r'\t|\r\n|\r|\n', '', regex=True)\
                    .str.strip()
    return column

def process_csv(csv_path):
    
    print(f'Processing file : {csv_path}')
    if not os.path.exists("Output"):
        os.makedirs("Output")
    initial_output_path = "Output/"
    try:
        processed_df = pd.read_csv('temp/' + csv_path, delimiter=';', encoding='latin1',low_memory=False)
        logging.info(f"File {csv_path} read successfully.")
        logging.info(f"Initial DataFrame shape: {processed_df.shape}")
        logging.info(f"Initial DataFrame head:\n{processed_df.head()}")
    except Exception as e:
        logging.error(f"Error reading the file {csv_path}: {e}")
        return
 
    if not processed_df.empty:
        for column in ['totalLineaNeto', 'precioNeto', 'cantidad']:
            try:
                processed_df[column] = processed_df[column].astype(str).str.replace(',', '.').astype(float)  
            except KeyError:
                logging.warning(f"Column {column} not found in {csv_path}")
 
        logging.info(f"DataFrame shape after replacing commas in : {processed_df.shape}")
        logging.info(f"DataFrame head after replacing commas in :\n{processed_df.head()}")
 
        columns_to_clean = [col for col in processed_df.columns if col not in [
            'cantidad', 'Descuentos', 'totalCargos', 'totalDescuentos',
            'FechaCreacion', 'FechaEnvio', 'FechaSolicitudCancelacion',
            'fechaUltimaModificacion', 'FechaAceptacion', 'FechaCancelacion'
        ]]

        print(f'Cleaning white spaces in files : {csv_path}')
        
        for col in tqdm(columns_to_clean):
            if processed_df[col].dtype == 'object':
                processed_df[col] = clean_column(processed_df[col])
        
        # processed_df[columns_to_clean] = processed_df[columns_to_clean].apply(clean_column)
        
        print("Cleaning White Spaces started..........")
        st = time.time()
        logging.info(f"DataFrame shape after cleaning White Spaces: {processed_df.shape}")
        logging.info(f"DataFrame head after cleaning White Spaces:\n{processed_df.head()}")
 
        processed_df['FechaEnvio'] = pd.to_datetime(processed_df['FechaEnvio'], errors='coerce')
        processed_df['Year'] = processed_df['FechaEnvio'].dt.year
        processed_df['Month'] = processed_df['FechaEnvio'].dt.strftime('%B')
 
        logging.info(f"DataFrame shape after adding Year and Month to new columns: {processed_df.shape}")
        logging.info(f"DataFrame head after adding Year and Month to new columns:\n{processed_df.head()}")
        ed = time.time()
        print(f"Time taken to clean white spaces: {ed - st} seconds")
        print("Cleaning White Spaces completed..........")
        
        try:
            st_s = time.time()
            print('Saving the csv file..........')
            formatted_time = datetime.now().strftime("%Y.%m.%d_%H.%M.%S_")
            final_file_path = initial_output_path + formatted_time + '' + csv_path
            processed_df.to_csv(final_file_path, index=False, encoding='utf-8-sig',chunksize=1000000)
            
            
            logging.info(f'Processed and cleaned data saved to {final_file_path}')
            logging.info(f"Final DataFrame shape: {processed_df.shape}")
            logging.info(f"Final DataFrame head:\n{processed_df.head()}")
            ed_s = time.time()
            print(f"Time taken to save the csv file: {ed_s - st_s} seconds")
            print('Saving the csv file completed..........')
        except Exception as e:
            logging.error(f"Error writing the file: {e}")
    else:
        logging.warning(f"No data to save for {csv_path}.")
 
def Cleaning_Data():
   
    if not os.path.exists("temp"):
        os.makedirs("temp")
       
    csv_files = os.listdir("temp")
    
    # for file in tqdm(csv_files):
    #     process_csv(file)
 
    # Create a progress bar
    with tqdm(total=len(csv_files), desc="Processing files") as progress_bar:
        def process_with_progress(csv_path):
            process_csv(csv_path)
            progress_bar.update(1)
 
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            executor.map(process_with_progress, csv_files)
 
# download_data()
Cleaning_Data()