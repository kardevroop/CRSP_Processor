import pandas as pd
from dataloader.stock import Stock
import numpy as np
import os

class DataLoader:
    def __init__(self, data_dir, company_ticker_file):
        # self.result_dir = result_dir
        self.data = pd.read_csv(data_dir, low_memory=False)
        self.data['CUSIP'] = self.data['CUSIP'].astype(str)
        self.data['TICKER'] = self.data['TICKER'].astype(str)

        self.company_tickers = self.read_words_from_file(company_ticker_file)
        self.protfolio = []
        print("successfully loaded data")
    
    def read_words_from_file(self, file_path):
        # Open the file and read lines
        with open(file_path, 'r') as file:
            # Reading each line and stripping newline characters
            words = [line.strip() for line in file]
        return words
    
    def load_data(self, ticker):
        # raw_data = pd.read_csv(self.file_path, parse_dates=["date"],low_memory=False)

        # for ticker in  (self.company_tickers):
            # print("company name: {}, ticker: {}".format(name, ticker))
        df = self.data[self.data['TICKER'] == ticker]
        # df = raw_data[raw_data['COMNAM'] == name]
        df = df.drop_duplicates(subset='date') # remove duplicate rows, it is not common but happens
        # if (ticker == 'DOW'):
        #     df = df[df['CUSIP'] == '26054310']
        if(ticker == 'GS'):
            df = df[df['CUSIP'] == '38141G10']
        elif(ticker == 'HON'):
            df = df[df['CUSIP'] == '43851610']
        elif(ticker == 'JPM'):
            df = df[df['CUSIP'] == '46625H10']
        elif(ticker == 'TRV'):
            df = df[df['CUSIP'] == '89417E10']
        elif(ticker == 'V'):
            df = df[df['CUSIP'] == '92826C83']
        elif(ticker == 'CRM'):
            df = df[df['CUSIP'] == '79466L30']
        df = df.sort_values(by='date')

        return df

    def create_for_portfolio(self):
        # Load the data for the companies in the company_tickers
        for ticker in self.company_tickers:
            stock_data = self.load_data(ticker)
            stock = Stock(ticker, stock_data)
            self.protfolio.append(stock)
    
    def add_predictors(self):
        for stock in self.protfolio:
            stock.replace_char_with_zero("RET")
            stock.add_volumn_change()
            stock.add_BA_Spread()
            stock.add_Illiquidity()
            stock.add_TurnOver()
            stock.add_Transaction_Cost()
            stock.add_Market_Cap()
        print("finished adding predictors")
    
    def select_columns(self, list_columns):
        for stock in self.protfolio:
            stock.select_columns(list_columns)
    
    def remove_nan(self):
        for stock in self.protfolio:
            stock.remove_nan()

    def save_stock_data(self, result_dir):
        # Save the stock data to the result_dir
        for stock in self.protfolio:
            stock.save_stock_data(result_dir)
    
    def split_train_validation_test(self):
        for stock in self.protfolio:
            stock.split_train_validation_test()
    
    def get_combined_data(self, data_type, parameters):
        combined_returns = pd.DataFrame()
        for stock in self.protfolio:
            if data_type == 'train':
                df = stock.train
            elif data_type == 'validation':
                df = stock.validation
            elif data_type == 'test':
                df = stock.test
            else:
                print("data type is not valid")
                return
            if combined_returns.empty:
                combined_returns = df[parameters].rename(columns={parameter: str(stock.ticker) + "_" + parameter for parameter in parameters})
                combined_returns['date'] = df['date']
            else:
                new_data = df[parameters].rename(columns={parameter: str(stock.ticker) + "_" + parameter for parameter in parameters})
                new_data['date'] = df['date']
                combined_returns = pd.merge(combined_returns, new_data, on='date', how='inner')
        # Get the column you want to move
        date_column = combined_returns.pop("date")
        insert_position = 0
        # Insert the column at the desired position
        combined_returns.insert(insert_position, "date", date_column)
        return combined_returns
        

    def save_combined_returns(self, result_dir):
        # Save the combined returns for the portfolio
        # inner join the returns of all stocks by date
        combined_return_train = self.get_combined_data('train', ['RET'])
        combined_return_validation = self.get_combined_data('validation', ['RET'])
        combined_return_test = self.get_combined_data('test', ['RET'])

        combined_return_train.to_csv(os.path.join(result_dir, "combined_returns_train.csv"), index=False)
        combined_return_validation.to_csv(os.path.join(result_dir, "combined_returns_validation.csv"), index=False)
        combined_return_test.to_csv(os.path.join(result_dir, "combined_returns_test.csv"), index=False)
        print("finished saving combined returns")
    
    def save_combined_parameters(self, result_dir, parameters, file_name):
        # Save the combined returns for the portfolio
        # inner join the returns of all stocks by date
        combined_parameters_train = self.get_combined_data('train', parameters)
        combined_parameters_validation = self.get_combined_data('validation', parameters)
        combined_parameters_test = self.get_combined_data('test', parameters)

        combined_parameters_train.to_csv(os.path.join(result_dir, f"combined_{file_name}_train.csv"), index=False)
        combined_parameters_validation.to_csv(os.path.join(result_dir, f"combined_{file_name}_validation.csv"), index=False)
        combined_parameters_test.to_csv(os.path.join(result_dir, f"combined_{file_name}_test.csv"), index=False)
        print("finished saving combined parameters")

    def set_train_validation_test_dates(self, start_train, end_train, start_validation, end_validation, start_test, end_test):
        for stock in self.protfolio:
            stock.set_train_validation_test_dates(start_train, end_train, start_validation, end_validation, start_test, end_test)
        print("finished setting train, validation, and test dates")
