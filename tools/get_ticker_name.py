import pandas as pd
from glob import glob

data_dir = "CRSP_Processor/sp_500_market_cap_selected"
files = glob(data_dir + "/*.png")
tickers = []
for file in files:
    tickers.append(file.split("selected/")[-1].split("_market")[0])
print(tickers)
with open("selected_tickers_50.txt", "w") as f:
    for ticker in tickers:
        f.write(ticker + "\n")
