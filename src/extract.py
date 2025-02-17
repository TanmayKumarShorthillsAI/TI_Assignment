import pandas as pd
import os
from flow import manage_flow
import gc


class LoadDataframe:
    def __init__(self, dir_path):
        self.dir = dir_path
        self.file_paths = []

    def get_file_names(self):
        for root, dirs, files in os.walk(self.dir):
            for file in files:
                if file.endswith("tsv.gz"):
                    file_path = root + file
                    self.file_paths.append(file_path)

    def make_df(self):
        self.get_file_names()
        df_list = []
        i = 1
        try:
            chunk_size = (10**5) * 3
            for names in self.file_paths:
                with pd.read_csv(
                    names,
                    sep="\t",
                    header=None,
                    on_bad_lines="skip",
                    compression="infer",
                    usecols=[1, 2, 4],
                    names=["Post_event_list", "Post_product_list", "URL"],
                    chunksize=chunk_size,
                ) as reader:
                    for chunk in reader:
                        print("Reading chunk number: ", i)
                        df_list.append(manage_flow(chunk))
                        print("Read chunk number: ", i)
                        i += 1
                        del chunk
                        # gc.collect()

                # df_list.append(df)

            final_df = pd.concat(df_list)
            print(final_df)
            return final_df

            # return combined_df
        except Exception as e:
            print(e)
