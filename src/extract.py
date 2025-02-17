import pandas as pd
import os
# from flow import manage_flow
import gc


class LoadFiles:
    def __init__(self, dir_path):
        self.dir = dir_path
        self.file_paths = []

    def get_file_names(self):
        for root, dirs, files in os.walk(self.dir):
            for file in files:
                if file.endswith("tsv.gz"):
                    file_path = root + file
                    self.file_paths.append(file_path)
        
        return self.file_paths
