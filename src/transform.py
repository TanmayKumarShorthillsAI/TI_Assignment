import pandas as pd

class TransformData():
    def __init__(self, df):
        self.df = df
        self.product_list = []
        pd.set_option('display.max_columns', 10)
        pd.set_option('display.width', 500)

    def drop_and_rename_columns(self):
        self.df.drop([3,5], axis = 1, inplace=True)

        self.df = self.df.rename(columns = {0:'Date', 1:'Post_event_list', 2:'Post_product_list', 4:'URL'})
        self.df.fillna('', inplace=True)
        # print(self.df.head())


    def filter_based_on_event_id(self, event_id):
        self.df = self.df[self.df['Post_event_list'].apply(lambda x: event_id in x.split(','))]
        
        # return event_filtered_df
    
    def expand_product_list(self):
        self.df["Post_product_list"] = self.df["Post_product_list"].apply(lambda x: x.split(','))
        self.df = self.df.explode(column=["Post_product_list"]).filter(["Post_product_list", "URL"])

    def make_new_records(self, arr, url):
        product_dict = {"Dealer_Id":'', "Ad_Id": '', "Impression_count": '', 'Domain_name':'', "Product_data": ''}

        try:
            product_dict['Domain_name'] = url.split('/')[2].split('.')[1]
            
            product_dict['Dealer_Id'] = arr[0]
            product_dict['Ad_Id'] = arr[1]
            product_dict['Impression_count'] = arr[4].split('|')[0].split('=')[-1]
            product_dict['Product_data'] = "|".join(arr[4].split('|')[1:]) + ";" + arr[5:]

            self.product_list.append(product_dict)
        except Exception as e:
            print(e)



    def produce_product_info_df(self):
        self.df["Post_product_list"] = self.df["Post_product_list"].apply(lambda x: x.split(';'))
        self.df.apply(lambda x: self.make_new_records(x.Post_product_list, x.URL), axis=1)

        product_info_df = pd.DataFrame(self.product_list)
        print(product_info_df)
        

        
