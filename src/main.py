from extract import LoadDataframe
from transform import TransformData
import pandas as pd
import gc

   

def main():
    load_dataframe = LoadDataframe("../assets/")
    final_df = load_dataframe.make_df()

    transform_obj = TransformData()
    groupped_df = transform_obj.impression_count_by_name(final_df)
    del final_df

    


if __name__ == "__main__":
    main()
