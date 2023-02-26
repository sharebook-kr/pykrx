from mpykrx import stock
import pandas as pd

df = stock.get_modified_market_ohlcv_by_ticker('20230224', market='ALL')
print(df.sample(5))


# df = stock.get_market_trading_volume_by_investor("20210115", "20210122", "005930")
# print(df.head())

# # low_list = [ ]
# # tickers = stock.get_market_ticker_list()
# # print(tickers)
# # for ticker in tickers:
# #     df = stock.get_market_ohlcv_by_date("20210103", "20210430", ticker)
# #     low_list.append(df['저가'])

# # df = pd.concat(low_list, axis=1)
# # print(df)
import mpykrx
# print(mpykrx.__version__)

# df = stock.get_market_price_change_by_ticker(fromdate="20210104", todate="20210111")
# print(df)

# print(stock.get_market_net_purchases_of_equities_by_ticker('20210801', '20210831', 'ALL', '기관합계'))
# df = stock.get_modified_market_ohlcv_by_ticker('20230224')
# print(len(df.sample()))
# print(df.sample(3))
# print(stock.is_holiday_df(df))
