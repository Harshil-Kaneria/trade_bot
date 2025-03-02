import time
import pandas as pd
from kite_trade import *
from kiteconnect import KiteTicker

enctoken = "QesklZhuXkLjpFboAvGPyYcdVJXw5C69riGfsDf98bDzeTtqzYHTUr2BJ5R+jeyiEJS+hkwxAmXT0m88MmlUocLrzpQ+0/Y8hOgkYCicfTuZMf2lq/PbiQ=="
kite = KiteApp(enctoken=enctoken)

symbols_list = list(pd.read_csv('data.csv')['SYMBOL \n'])

df = pd.DataFrame(kite.instruments("NSE"))
df = df[df["tradingsymbol"].isin(symbols_list)].reset_index(drop=True)
df = df[["instrument_token","tradingsymbol","segment"]]

df["LTP"] = None
df["Open"] = None
df["High"] = None
df["Low"] = None
df["PrevClose"] = None
df["AvgPrice"] = None
df["TradedValue"] = None
df["Volume"] = None
df["Gap%"] = None
df["ChangePrev%"] = None
df["ChangeToday%"] = None

socket = False
user_id = kite.profile()["user_id"]
kws = KiteTicker(api_key="your_api_key", access_token=enctoken + "&user_id=" + user_id)

def on_ticks(ws, ticks):
    print(ticks)

def on_connect(ws, response):
    global socket
    socket = True
    kws.subscribe(list(df["instrument_token"]))

kws.on_ticks = on_ticks
kws.on_connect = on_connect
kws.connect(threaded=True)

while not socket:
    time.sleep(1)

print("WebSocket: Connected")

time.sleep(10)