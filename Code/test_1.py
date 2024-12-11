from Chile_Data_Acquisition_Cenabast.Code.part_2 import Mapping_File_Format
import pandas as pd

df = pd.read_csv(r"D:\Downloads\2024.12.11_13.16.44_2024-10.csv")

final_data , final_Combinacion_Vertical = Mapping_File_Format(df)

final_data.to_csv("final_data.csv", index=False, encoding='utf-8-sig')

final_Combinacion_Vertical.to_csv("final_Combinacion_Vertical.csv", index=False, encoding='utf-8-sig')

