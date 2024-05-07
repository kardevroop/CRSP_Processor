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

if __name__ == '__main__':
    # Load the data
    data_dir = "/Users/zimenglyu/Documents/datasets/CRSP/sp500/sp500_small.csv"
    test_year = 2022
    # data = pd.read_csv(data_dir)
    # parameters = list_columns = ["date", "TICKER","CUSIP", "COMNAM", "RET", "VOL" , "sprtrn",   "PRC", "SHROUT", "ASK", "BID"]
    # data = data[parameters]
    # data.to_csv("/Users/zimenglyu/Documents/datasets/CRSP/sp500/sp500_small.csv", index=False)
    company_ticker_file = "/Users/zimenglyu/Documents/datasets/CRSP/sp500/sp_500.txt"
    # company_ticker_file = "/Users/zimenglyu/Documents/code/git/CRSP_Processor/selected_tickers_50.txt"
    # company_tickers = read_words_from_file(company_ticker_file)
    # print(len(company_tickers))
    # print(company_tickers)
    parameters = list_columns = ["date", "TICKER", "CUSIP", "COMNAM", "RET", "VOL_CHANGE", "BA_SPREAD", "ILLIQUIDITY", "sprtrn", "TURNOVER",  "PRC", "SHROUT", "MARKET_CAP","TRAN_COST", "ASK", "BID"]

    start_train = "1990-01-01"
    end_train = f"{test_year-2}-12-31"
    start_validation = f"{test_year-1}-01-01"
    end_validation = f"{test_year-1}-12-31"
    start_test = f"{test_year}-01-01"
    end_test = f"{test_year}-12-31"

    result_dir = f"./{test_year}_sp_500"
    create_dir(result_dir)
    create_dir(os.path.join(result_dir, "train"))
    create_dir(os.path.join(result_dir, "validation"))
    create_dir(os.path.join(result_dir, "test"))
    create_dir(os.path.join(result_dir, "raw_data"))

    data_loader = DataLoader(data_dir, company_ticker_file)
    data_loader.create_for_portfolio()
    data_loader.add_predictors()
    data_loader.select_columns(parameters)
    
    data_loader.remove_nan()
    data_loader.set_train_validation_test_dates(start_train, end_train, start_validation, end_validation, start_test, end_test)
    data_loader.split_train_validation_test()
    data_loader.save_stock_data(result_dir)
    data_loader.save_combined_returns(result_dir)
    crsp_parameters = ["RET", "VOL_CHANGE", "ASK", "BID", "sprtrn"]
    predictors = ["BA_SPREAD", "ILLIQUIDITY", "TURNOVER", "VOL_CHANGE", "RET", "sprtrn"]
    data_loader.save_combined_parameters(result_dir, crsp_parameters, "parameters")
    data_loader.save_combined_parameters(result_dir, predictors, "predictors")
    print("finished saving data")


