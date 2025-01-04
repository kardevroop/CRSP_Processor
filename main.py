import pandas as pd
from dataloader.dataloader import DataLoader
import os

def read_words_from_file(file_path):
    # Open the file and read lines
    with open(file_path, 'r') as file:
        # Reading each line and stripping newline characters
        words = [line.strip() for line in file]
    return words

def create_dir(dir_name):
    if not os.path.exists(dir_name):
        os.makedirs(dir_name)

def get_class_B_stocks(data, ticker):
    if ticker == "BRK" or ticker == "BF":
        data = data[data["SHRCLS"] == "B"]
    return data

def find_permco(tickers, data_path):
    data = pd.read_csv(data_path, low_memory=False)
    print(f"length of data: {len(data)}")
    data['PERMNO'] = data['PERMNO'].astype(str)
    data['TICKER'] = data['TICKER'].astype(str)
    data['SHRCLS'] = data['SHRCLS'].astype(str)
    data['date'] = pd.to_datetime(data['date'])
    stock_info = {}
    # tickers = ["AOS"]
    for ticker in tickers:
        # 1. get tickers first, it might contain different permno codes
        filter_data = data[data['TICKER'] == ticker]
        filter_data = get_class_B_stocks(filter_data, ticker)
        # 2. since the PERMMO never change, we only care about the latest PERMNO
        filter_data = filter_data.sort_values(by='date')
        filter_data = filter_data.tail(10)
        # 3. get the PERMNO
        unique_values = filter_data['PERMNO'].unique()
        # print(f"Ticker {ticker} has {len(unique_values)} permno values, and they are: {unique_values}")
        if len(unique_values) > 1:
            # print(f"Ticker {ticker} has {len(unique_values)} permno values, and they are: {unique_values}")
            # print("Picking class A stock information only!")
            if "A" in filter_data['SHRCLS'].unique():
                filter_data = filter_data[filter_data['SHRCLS'] == 'A']
            elif "V" in filter_data['SHRCLS'].unique():
                # V means voting shares, not publicly trade stocks
                filter_data = filter_data[filter_data['SHRCLS'] != 'V']
            else:
                print(f"Ticker {ticker} has {len(unique_values)} permno values, and they are: {unique_values}")
                print(f"Ticker {ticker} has no class A stock or class V, this should not happen!")
                filter_data.to_csv(f"./{ticker}_check.csv", index=False)
                print(f"File {ticker}_check.csv has been saved, please check it!")
            #  we are doing it here because if there is only one class, the column is empty
            unique_values = filter_data['PERMNO'].unique()
        if len(unique_values) == 1:
            stock_info[ticker] = unique_values[0]
        elif len(unique_values) == 0:
            print(f"Ticker {ticker} permno code not found, this should not happen!")
        else:
            print(f"Ticker {ticker} has {len(unique_values)} permno values, and they are: {unique_values}")


        # # check data date
        latest_date = filter_data['date'].max()

        if pd.isnull(latest_date):
            print(f"Ticker {ticker} has no data, this should not happen!")
            continue
        if latest_date.year < 2023:
            print(f"Ticker {ticker} has data until {latest_date}, this should not happen!")
            print

    return stock_info

def dict_to_csv(dict, file_path):
    df = pd.DataFrame(list(dict.items()),columns = ['TICKER','PERMNO'])
    df.to_csv(file_path, index=False)

def filter_large_data (new_data_path):
    old_data_path = "/Users/zimenglyu/Documents/datasets/CRSP/sp500/sp500_new.csv"
    parameters = ["date", "TICKER", "PERMNO", "COMNAM", "SHRCLS", "NAMEENDT", "RET", "VOL" , "sprtrn", "PRC", "SHROUT", "ASK", "BID"]
    data = pd.read_csv(old_data_path, low_memory=False)
    data = data[parameters]
    data.to_csv(new_data_path, index=False)

def csv_to_dict(file_path):
    df = pd.read_csv(file_path)
    return dict(zip(df.ticker, df.permco))


if __name__ == '__main__':
    # Load the data
    test_year = 2022
    data_path = "/Users/zimenglyu/Documents/datasets/CRSP/sp500/sp500_new.csv"
    
    # data_path = "/Users/zimenglyu/Documents/datasets/CRSP/sp500/sp500_small.csv"
    # company_ticker_file = "/Users/zimenglyu/Documents/datasets/CRSP/sp500/sp_500_for_CRSP.txt"
    company_ticker_file = "/Users/zimenglyu/Documents/code/git/CRSP_Processor/selected_tickers_50.txt"
    permco_csv_path = "./sp_500_permco_info.csv"
    
    parameters = ["date", "TICKER", "PERMNO", "COMNAM", "SHRCLS", "NAMEENDT", "RET", "VOL_CHANGE", "BA_SPREAD", "ILLIQUIDITY", "sprtrn", "TURNOVER",  "PRC", "SHROUT", "MARKET_CAP","TRAN_COST", "ASK", "BID"]
    input_parameters = ["RET",  "VOL_CHANGE",  "BA_SPREAD",  "ILLIQUIDITY", "sprtrn", "TURNOVER"]
    start_train = "1990-01-01"
    end_train = f"{test_year-2}-12-31"
    start_validation = f"{test_year-1}-01-01"
    end_validation = f"{test_year-1}-12-31"
    start_test = f"{test_year}-01-01"
    end_test = f"{test_year}-12-31"

    result_dir = f"./{test_year}_sp_500_select_50"
    create_dir(result_dir)
    create_dir(os.path.join(result_dir, "train"))
    create_dir(os.path.join(result_dir, "validation"))
    create_dir(os.path.join(result_dir, "test"))
    create_dir(os.path.join(result_dir, "raw_data"))

    # filter_large_data(data_path)
    company_tickers = read_words_from_file(company_ticker_file)
    print(company_tickers)
    print(f"length of company_tickers: {len(company_tickers)}")
    permco_info = find_permco(company_tickers, data_path)
    dict_to_csv(permco_info, permco_csv_path)
    print(f"size of permco_info: {len(permco_info)}")
    # start
    print(f"Prepare the data for {test_year}")
    data_loader = DataLoader(data_path, company_ticker_file, permco_info)
    data_loader.create_for_portfolio()
    data_loader.add_predictors()
    data_loader.select_columns(parameters)
    # data_loader.sanity_check_time_diff(90)
    
    data_loader.remove_nan(input_parameters)
    data_loader.save_raw_data(result_dir)
    data_loader.set_train_validation_test_dates(start_train, end_train, start_validation, end_validation, start_test, end_test)
    data_loader.split_train_validation_test()
    data_loader.save_stock_data(result_dir)


    data_loader.save_combined_returns(result_dir)
    crsp_parameters = ["RET", "VOL_CHANGE", "ASK", "BID", "sprtrn"]
    predictors = ["BA_SPREAD", "ILLIQUIDITY", "TURNOVER", "VOL_CHANGE", "RET", "sprtrn"]
    data_loader.save_combined_parameters(result_dir, crsp_parameters, "parameters")
    data_loader.save_combined_parameters(result_dir, predictors, "predictors")
    print("finished saving data")


