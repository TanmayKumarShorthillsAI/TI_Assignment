import pandas as pd
import os

class LoadDataframe:
    def __init__(self, dir_path):
        self.dir = dir_path
        self.file_paths = []
        
    def get_file_names(self):
        for root,dirs,files in os.walk(self.dir):
            for file in files:
                if file.endswith("tsv.gz"):
                    file_path = root + file
                    self.file_paths.append(file_path)


    def make_df(self):
        self.get_file_names()
        df_list = []

        try:
            for names in self.file_paths:
                df = pd.read_csv(names, sep='\t', header=None, on_bad_lines='skip', compression='infer')
                df_list.append(df)

            combined_df = pd.concat(df_list)
            print(combined_df.head())

            return combined_df
        except Exception as e:
            print(e)
        