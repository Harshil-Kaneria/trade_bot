import time
import pandas as pd
from kite_trade import *
from kiteconnect import KiteTicker

enctoken = "QesklZhuXkLjpFboAvGPyYcdVJXw5C69riGfsDf98bDzeTtqzYHTUr2BJ5R+jeyiEJS+hkwxAmXT0m88MmlUocLrzpQ+0/Y8hOgkYCicfTuZMf2lq/PbiQ=="
kite = KiteApp(enctoken=enctoken)

symbols_list = list(pd.read_csv('data.csv')['SYMBOL \n'])
symbols_list.append("NIFTY BANK")

df = pd.DataFrame(kite.instruments("NSE"))
df = df[df["tradingsymbol"].isin(symbols_list)].reset_index(drop=True)
df = df[["instrument_token","tradingsymbol","segment"]]

df["LTP"] = None
df["Open"] = None
df["High"] = None
df["Low"] = None
df["PrevClose"] = None
df["AvgPrice"] = None
df["Volume"] = None
df["TradedValue"] = None
df["Volatility"] = None
df["Gap%"] = None
df["ChangePrev%"] = None
df["ChangeToday%"] = None

df = df.set_index("instrument_token")

socket = False
user_id = kite.profile()["user_id"]
kws = KiteTicker(api_key="your_api_key", access_token=enctoken + "&user_id=" + user_id)

def updateData(i):
    df.loc[i['instrument_token'],"LTP"] = i["last_price"]
    df.loc[i['instrument_token'],"Open"] = i["ohlc"]["open"]
    df.loc[i['instrument_token'],"High"] = i["ohlc"]["high"]
    df.loc[i['instrument_token'],"Low"] = i["ohlc"]["low"]
    df.loc[i['instrument_token'],"PrevClose"] = i["ohlc"]["close"]
    df.loc[i['instrument_token'], "Gap%"] = ((df.loc[i['instrument_token'], "Open"] - df.loc[i['instrument_token'], "PrevClose"]) / df.loc[i['instrument_token'], "PrevClose"]) * 100
    df.loc[i['instrument_token'], "ChangePrev%"] = ((df.loc[i['instrument_token'], "LTP"] - df.loc[i['instrument_token'], "PrevClose"]) / df.loc[i['instrument_token'], "PrevClose"]) * 100
    df.loc[i['instrument_token'], "ChangeToday%"] = ((df.loc[i['instrument_token'], "LTP"] - df.loc[i['instrument_token'], "Open"]) / df.loc[i['instrument_token'], "Open"]) * 100
    df.loc[i['instrument_token'], "Volatility"] = ((df.loc[i['instrument_token'], "High"]-df.loc[i['instrument_token'], "Low"])/df.loc[i['instrument_token'], "Open"])*100
    try:
        df.loc[i['instrument_token'],"AvgPrice"] = i["average_traded_price"]
        df.loc[i['instrument_token'],"Volume"] = i["volume_traded"]
        df.loc[i['instrument_token'], "TradedValue"] = df.loc[i['instrument_token'], "AvgPrice"] * df.loc[i['instrument_token'], "Volume"]
    except:
        pass

def on_ticks(ws, ticks):
    for i in ticks:
        updateData(i)

def on_connect(ws, response):
    global socket
    socket = True
    kws.subscribe(list(df.index))

kws.on_ticks = on_ticks
kws.on_connect = on_connect
kws.connect(threaded=True)

while not socket:
    time.sleep(1)

print("WebSocket: Connected")

while True:
    print(df)
    time.sleep(1)