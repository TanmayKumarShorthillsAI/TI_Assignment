from extract import LoadFiles
from transform import TransformData
import pandas as pd
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

    domain_groupped_df, dealer_groupped_df = (
        transform_dataframe.impression_count_by_name(product_df=product_info_df)
    )
    del product_info_df

    return domain_groupped_df, dealer_groupped_df


def main():
    load_files = LoadFiles("../assets/")
    names = load_files.get_file_names()
    domain_groupped_df_list = []
    dealer_groupped_df_list = []

    try:
        for name in names:
            df = pd.read_csv(
                name,
                sep="\t",
                header=None,
                on_bad_lines="skip",
                compression="infer",
                usecols=[1, 2, 4],
                names=["Post_event_list", "Post_product_list", "URL"],
            )
            dfs = manage_flow(df)
            domain_groupped_df_list.append(dfs[0])
            dealer_groupped_df_list.append(dfs[1])

    except Exception as e:
        print(e)

    # final = pd.DataFrame()

    print(len(domain_groupped_df_list), domain_groupped_df_list[0].head())
    print(len(dealer_groupped_df_list), dealer_groupped_df_list[0].head())

    domain_groupped_final_df = (
        pd.concat(domain_groupped_df_list).groupby("Domain_name").sum()
    )
    del domain_groupped_df_list

    dealer_groupped_final_df = (
        pd.concat(dealer_groupped_df_list).groupby("Dealer_Id").sum()
    )

    del dealer_groupped_df_list

    print(domain_groupped_final_df)
    print(dealer_groupped_final_df)

    # del final_df


if __name__ == "__main__":
    main()
