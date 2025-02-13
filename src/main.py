from extract import LoadDataframe
from transform import TransformData
import pandas as pd


def main():
    load_dataframe = LoadDataframe("../assets/")
    combined_df = load_dataframe.make_df()
    transform_dataframe = TransformData(combined_df)

    transform_dataframe.drop_and_rename_columns()
    event_id = "20113"
    transform_dataframe.filter_based_on_event_id(event_id=event_id)
    transform_dataframe.expand_product_list()
    transform_dataframe.produce_product_info_df()


if __name__ == "__main__":
    main()
