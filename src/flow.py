from transform import TransformData
import gc


def manage_flow(df):
    transform_dataframe = TransformData()

    print(df.info(memory_usage="deep"))
    transform_dataframe.fill_na_values(df)

    event_id = "20113"
    transform_dataframe.filter_based_on_event_id(event_id=event_id, combined_df=df)
    expanded_df = transform_dataframe.expand_product_list(combined_df=df)
    print()
    del df
    gc.collect()

    product_info_df = transform_dataframe.produce_product_info_df(expanded_df)
    del expanded_df
    gc.collect()
    print(product_info_df.info(memory_usage="deep"))

    # groupped_df = transform_dataframe.impression_count_by_name(
    #     product_df=product_info_df
    # )
    return product_info_df
    # del product_info_df
    # gc.collect()

    # transform_dataframe.impression_count_by_name(product_df=product_info_df)
