import pandas as pd
import os
import matplotlib.pyplot as plt


def read_words_from_file(file_path):
    # Open the file and read lines
    with open(file_path, 'r') as file:
        # Reading each line and stripping newline characters
        words = [line.strip() for line in file]
    return words

def create_dir(dir_name):
    if not os.path.exists(dir_name):
        os.makedirs(dir_name)

def plot_market_cap(df, ticker, result_dir,  start_date='1990-01-01'):
    # Convert the "data" column to datetime format
    df['date'] = pd.to_datetime(df['date'])
    start = pd.to_datetime(start_date)
    filtered_df = df[df['date'] >= start]

    plt.figure(figsize=(12, 6))
    plt.plot(filtered_df['date'], filtered_df['MARKET_CAP'])
    plt.tight_layout()
    plt.xlabel('Date')
    plt.ylabel('Market Cap')

    # plt.ylim(0,1e10)
    plt.xticks(rotation=45)  # Rotate x-axis labels for better readability
    plt.grid(True)
    plt.title('{} Market Cap'.format(ticker))
    plt.savefig(os.path.join(result_dir, "{}_market_cap.png".format(ticker)), bbox_inches='tight', pad_inches=0.5)

def plot_all_market_caps(company_tickers, data_dir, result_dir):
    for ticker in company_tickers:
        df = pd.read_csv(os.path.join(data_dir, "{}.csv".format(ticker)))
        plot_market_cap(df, ticker, result_dir)
        time_diff = check_time_diff(df)
        if time_diff > 90:
            print(f"Time diff of {ticker} is {time_diff} days")

def plot_small_market_caps(company_tickers, data_dir, result_dir):
    # Three conditions:
    # 1. The maximum market cap is less than 9e7
    # 2. The market cap history since 2010 or earlier
    # 3. time diff between two consecutive dates is less than three months
    start_date = "2000-01-01"
    for ticker in company_tickers:
        df = pd.read_csv(os.path.join(data_dir, "{}.csv".format(ticker)))
        if df['MARKET_CAP'].max() < 9e7:
            if df['date'].min() <= '2010-01-01':
                if not check_time_diff(df, 90):
                    plot_market_cap(df, ticker, result_dir, start_date)
                else: 
                    print("Time diff is greater than 3 months for {}".format(ticker))

def check_time_diff(df):
    # Convert the "data" column to datetime format
    df['date'] = pd.to_datetime(df['date'])
    time_diff = df['date'].diff().dt.days
    
    return time_diff.max()

if __name__ == '__main__':
    data_dir = "./2023_sp_500/raw_data"
    result_dir = "./sp_500_marketcap"
    create_dir(result_dir)
    company_ticker_file = "/Users/zimenglyu/Documents/datasets/CRSP/sp500/sp_500_for_CRSP.txt"
    company_tickers = read_words_from_file(company_ticker_file)

    plot_all_market_caps(company_tickers, data_dir, result_dir)
    # plot_small_market_caps(company_tickers, data_dir, result_dir)

