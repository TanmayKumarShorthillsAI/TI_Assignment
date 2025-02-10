import pandas as pd

class TransformData():
    def __init__(self, df):
        self.df = df
        pd.set_option('display.max_columns', 10)
        pd.set_option('display.width', 500)

    def drop_and_rename_columns(self):
        self.df.drop([3,5], axis = 1, inplace=True)

        self.df = self.df.rename(columns = {0:'Date', 1:'Post_event_list', 2:'Post_product_list', 4:'URL'})
        self.df.fillna('', inplace=True)


    def filter_based_on_event_id(self, event_id) -> pd.DataFrame:
        event_filtered_df = self.df[self.df['Post_event_list'].apply(lambda x: event_id in x.split(','))]
        
        return event_filtered_df
    
    def expand_product_list(self, event_filtered_df):
        post_product_list = event_filtered_df["Post_product_list"].apply(lambda x: x.split(';;;'))
        product_list_df = post_product_list.explode()
        print(product_list_df.head())

        
