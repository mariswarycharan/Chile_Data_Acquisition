import os
from tqdm import tqdm
from part_2 import fuzzy_match_filtering
from part_3 import sucursal_matching
import pandas as pd
from datetime import datetime
from Chile_Data_Acquisition_Cenabast.Code.Chile import Chile_Data_Acquisition_Cenabast_data


csv_files = os.listdir("temp")
initial_output_path = "Output"

if not os.path.exists(initial_output_path):
    os.makedirs(initial_output_path)

Control_File_Month = pd.read_excel(r"Control\Control_File.xlsx" , sheet_name="Month")
    
Cenabast_File_Url_list = list(zip(Control_File_Month['Cenabast_File_Url'].to_list(),Control_File_Month['Month'].to_list()))
    
for file,Cenabast_File_Url in tqdm(zip(csv_files,Cenabast_File_Url_list)):
    
    df = pd.read_csv('temp/' + file)
    
    df = fuzzy_match_filtering(df,"")
    
    Pharmatender_final_df , chile_combined_df , final_df_merged_desintaria = sucursal_matching(df,datetime.now().strftime("%Y.%m.%d_%H.%M.%S_"))

    chile_combined_df_2 , final_df_merged_desintaria_2 = Chile_Data_Acquisition_Cenabast_data(Cenabast_File_Url)
    
    chile_combined_df = pd.concat([chile_combined_df , chile_combined_df_2] , ignore_index=True)
    
    
    final_df_merged_desintaria = pd.concat([final_df_merged_desintaria , final_df_merged_desintaria_2] , ignore_index=True)
    
    formatted_time = datetime.now().strftime("%Y.%m.%d_%H.%M.%S_")
    final_file_path = initial_output_path + formatted_time + '' + file

    if not os.path.exists(initial_output_path + "/Pharmatender_Format"):
        os.makedirs(initial_output_path + "/Pharmatender_Format")
        
    Pharmatender_final_df.to_csv(initial_output_path + "/Pharmatender_Format/" + formatted_time + '' + file, index=False, encoding='utf-8-sig',chunksize=1000000)
                
    if not os.path.exists(initial_output_path + "/Chile_Combined_Format"):
        os.makedirs(initial_output_path + "/Chile_Combined_Format")
        
    chile_combined_df.to_csv(initial_output_path + "/Chile_Combined_Format/" + formatted_time + '' + file, index=False, encoding='utf-8-sig',chunksize=1000000)
                
    if not os.path.exists(initial_output_path + "/Combinación_Vertical"):
        os.makedirs(initial_output_path + "/Combinación_Vertical")
        
    final_df_merged_desintaria.to_csv(initial_output_path + "/Combinación_Vertical/" + formatted_time + '' + file, index=False, encoding='utf-8-sig',chunksize=1000000)
    