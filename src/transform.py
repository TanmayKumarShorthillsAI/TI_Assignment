import pandas as pd
import gc


class TransformData:
    def __init__(self):
        # self.df = df
        self.product_list = []
        pd.set_option("display.max_columns", 10)
        pd.set_option("display.width", 500)

    def fill_na_values(self, combined_df):
        combined_df.fillna("", inplace=True)

    def filter_based_on_event_id(self, event_id, combined_df):
        combined_df = combined_df[
            combined_df["Post_event_list"].apply(lambda x: event_id in x.split(","))
        ]

        # return event_filtered_df

    def expand_product_list(self, combined_df):
        combined_df["Post_product_list"] = combined_df["Post_product_list"].apply(
            lambda x: x.split(",")
        )
        expanded_df = combined_df.explode(column=["Post_product_list"]).filter(
            ["Post_product_list", "URL"]
        )
        return expanded_df
        # print(self.df.head())

    def make_new_records(self, arr, url):
        product_dict = {
            "Dealer_Id": "",
            "Ad_Id": "",
            "Impression_count": "",
            "Domain_name": "",
            "Product_data": "",
        }

        try:
            product_dict["Domain_name"] = url.split("/")[2].split(".")[1]

            product_dict["Dealer_Id"] = arr[0]
            product_dict["Ad_Id"] = arr[1]
            product_dict["Impression_count"] = arr[4].split("|")[0].split("=")[-1]
            product_dict["Product_data"] = (
                "|".join(arr[4].split("|")[1:]) + ";" + arr[5]
            )

            self.product_list.append(product_dict)
        except Exception as e:
            pass

    def produce_product_info_df(self, expanded_df):
        expanded_df["Post_product_list"] = expanded_df["Post_product_list"].apply(
            lambda x: x.split(";")
        )
        expanded_df.apply(
            lambda x: self.make_new_records(x.Post_product_list, x.URL), axis=1
        )

        product_info_df = pd.DataFrame(self.product_list)
        product_info_df["Impression_count"] = pd.to_numeric(
            product_info_df["Impression_count"], downcast="signed"
        )

        product_info_df.dropna(how="all", inplace=True)
        del expanded_df
        del self.product_list
        gc.collect()
        return product_info_df
        # print(product_info_df)

    def impression_count_by_name(self, product_df):
        impression_by_domain_df = product_df.groupby("Domain_name").agg(
            impressions=("Impression_count", "sum")
        )

        impression_by_dealer_id = product_df.groupby("Dealer_Id").agg(
            impressions=("Impression_count", "sum")
        )
        print(impression_by_domain_df)
        print(impression_by_dealer_id)



