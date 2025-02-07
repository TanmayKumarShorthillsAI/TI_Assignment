from load import LoadDataframe;


def main():
    load_dataframe = LoadDataframe("../input_files/")
    combined_df = load_dataframe.make_df()

if __name__ == "__main__":
    main()