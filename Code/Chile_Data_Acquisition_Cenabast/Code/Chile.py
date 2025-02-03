import pandas as pd
import os
from tqdm import tqdm
from Chile_Data_Acquisition_Cenabast.Code.part_1 import fuzzy_match_filtering
from Chile_Data_Acquisition_Cenabast.Code.part_2 import Mapping_File_Format
from datetime import datetime
import gdown

def download_google_sheet(url , Month):
    try:
        if not os.path.exists("temp"):
            os.makedirs("temp")
                
        # Convert to direct download link
        file_id = url.split('/d/')[1].split('/')[0]
        direct_link = f'https://docs.google.com/uc?export=download&id={file_id}'
        # Download the Google Sheet as CSV
        gdown.download(direct_link, f'temp/{Month}.csv', quiet=False)
        print(f'File downloaded successfully: {Month}.csv')
    except Exception as e:
        print(f'Error downloading google sheet: {e}')

def Chile_Data_Acquisition_Cenabast_data(Url_Month_turple):
        
        Cenabast_File_Url , Month = Url_Month_turple
        
        download_google_sheet(Cenabast_File_Url , Month)
                
        df = pd.read_excel( "temp/" + Month + ".xlsx" )
        
        Month = int(str(Month).split("-")[1])
        
        filter_month_df = df[df['Mes'].isin([Month])]
        
        fuzzy_match_filtering_df = fuzzy_match_filtering( filter_month_df ,datetime.now().strftime("%Y.%m.%d_%H.%M.%S_"))
        
        final_data , final_Combinacion_Vertical = Mapping_File_Format(fuzzy_match_filtering_df)
        
        # formatted_time = datetime.now().strftime("%Y.%m.%d_%H.%M.%S_")

        # if not os.path.exists(initial_output_path + "/Chile_Combined_Format"):
        #     os.makedirs(initial_output_path + "/Chile_Combined_Format")
            
        # final_data.to_csv(initial_output_path + "/Chile_Combined_Format/" + formatted_time + '' + csv_path, index=False, encoding='utf-8-sig',chunksize=1000000)
               
        # if not os.path.exists(initial_output_path + "/Combinación_Vertical"):
        #     os.makedirs(initial_output_path + "/Combinación_Vertical")
            
        # final_Combinacion_Vertical.to_csv(initial_output_path + "/Combinación_Vertical/" + formatted_time + '' + csv_path, index=False, encoding='utf-8-sig',chunksize=1000000)
            
        return final_data , final_Combinacion_Vertical
        

        