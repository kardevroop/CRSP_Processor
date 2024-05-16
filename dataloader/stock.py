import pandas as pd
import os
import numpy as np

class Stock:
    def __init__(self, ticker, data):

        self.data = data
        self.ticker = ticker

    def add_volumn_change(self):
        # df['VOL_CHANGE'] = df['VOL'].diff()
        # return df
        self.data['VOL_CHANGE'] = self.data['VOL'].pct_change(fill_method=None)

    def add_BA_Spread(self):
        self.data['BA_SPREAD'] = (self.data['ASK'] - self.data['BID'])/self.data['PRC']

    def add_Illiquidity(self):
        self.data['ILLIQUIDITY'] = self.data['RET'] /(self.data['VOL'] * self.data['PRC'])
    def add_TurnOver(self):
        self.data['TURNOVER'] = self.data['VOL'] / self.data['SHROUT']

    def add_Transaction_Cost(self):
        self.data['TRAN_COST'] = (self.data['ASK'] - self.data['BID']) / 2    
    
    def add_Market_Cap(self):
        self.data['MARKET_CAP'] = self.data['PRC'] * self.data['SHROUT']
    
    def save_raw_data(self, result_dir):
        self.data.to_csv(os.path.join(result_dir, "raw_data/{}.csv".format(self.ticker)), index=False)
        
    def save_stock_data(self, result_dir):
        # self.data.to_csv(os.path.join(result_dir, "{}.csv".format(self.ticker)), index=False)
        self.train.to_csv(os.path.join(result_dir, "train/{}.csv".format(self.ticker)), index=False)
        self.validation.to_csv(os.path.join(result_dir, "validation/{}.csv".format(self.ticker)), index=False)
        self.test.to_csv(os.path.join(result_dir, "test/{}.csv".format(self.ticker)), index=False)

    def replace_char_with_zero(self, column_name):
        # Convert the column to string, replace 'c' with '0', and convert back to float
        if self.data[column_name].astype(str).str.contains('B').any():
            print(f"Found 'B' in column {column_name} of {self.ticker}")
            print (self.data[self.data[column_name].astype(str).str.contains('B').any()])
        self.data[column_name] = self.data[column_name].astype(str).str.replace('B', '0').astype(float)  
        self.data[column_name] = self.data[column_name].astype(str).str.replace('C', '0').astype(float)

    def select_columns(self, list_columns):
        self.data = self.data[list_columns]

    def remove_nan(self):
        self.data.replace([np.inf, -np.inf], np.nan, inplace=True)
        self.data.dropna(inplace=True)
    
    def split_train_validation_test(self):
        # Filter the DataFrame to include only data from 1990 to 2021
        self.data['date'] = pd.to_datetime(self.data['date'])
        self.train = self.data[(self.data['date'] >= self.train_start) & (self.data['date'] <= self.train_end)]
        self.validation = self.data[(self.data['date'] >= self.validation_start) & (self.data['date'] <= self.validation_end)]
        self.test = self.data[(self.data['date'] >= self.test_start) & (self.data['date'] <= self.test_end)]
        print("company {:<6}, train: {:<5}, validation: {:<5}, test: {:<5}".format(self.ticker, len(self.train), len(self.validation), len(self.test)))

    def set_train_validation_test_dates(self, start_train, end_train, start_validation, end_validation, start_test, end_test):
        self.train_start = pd.to_datetime(start_train)
        self.train_end = pd.to_datetime(end_train)
        self.validation_start = pd.to_datetime(start_validation)
        self.validation_end = pd.to_datetime(end_validation)
        self.test_start = pd.to_datetime(start_test)
        self.test_end = pd.to_datetime(end_test)
    
    def sanity_check_time_diff(self, threshold):
        self.data['date'] = pd.to_datetime(self.data['date'])
        time_diff = self.data['date'].diff().dt.days
        if time_diff.max() > threshold:
            print(f"Time diff of {self.ticker} is {time_diff.max()} days, getting rid of old data")
            # find the first date after the time_diff.max() and set it as the start date
            new_state_date = self.data['date'].iloc[time_diff.idxmax()]
            self.data = self.data[self.data['date'] > new_state_date]
            
        return time_diff.max()