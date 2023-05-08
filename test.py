# from twelvedata import TDClient

# # Initialize client - apikey parameter is requiered
# td = TDClient(apikey="2efb4e0c80a041b794abaf4369e76869")

# # Construct the necessary time series
# ts = td.time_series(
#     symbol="AAPL",
#     interval="1h",
#     date="2019-01-08",
#     timezone="America/New_York",
# )

# # Returns pandas.DataFrame
# ts.as_pandas().to_csv('AAPL_data.csv', header = True)

import requests

url = 'https://api.twelvedata.com/time_series'
params = {'symbol': 'AAPL', 'interval': '1h','date':'2019-01-08', 'apikey': '2efb4e0c80a041b794abaf4369e76869', 'timezone':'America/New_York'}

response = requests.get(url, params=params)

if response.status_code == requests.codes.ok:
    data = response.json()
    print(data)
else:
    print('Error:', response.status_code, response.text)

