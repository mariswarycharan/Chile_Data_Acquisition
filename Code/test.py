import os
from tqdm import tqdm
from part_2 import fuzzy_match_filtering
from part_3 import sucursal_matching
import pandas as pd

csv_files = os.listdir("temp")
    
for file in tqdm(csv_files):
    df = pd.read_csv('temp/' + file)
    
    df = fuzzy_match_filtering(df,"")
    df = sucursal_matching(df,"")
    
    df.to_csv('Output/' + file, index=False, encoding='utf-8-sig',chunksize=1000000)
            