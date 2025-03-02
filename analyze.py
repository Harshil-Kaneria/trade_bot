import os
from dotenv import load_dotenv
import time,datetime
import pandas as pd
from kite_trade import *
from kiteconnect import KiteTicker

load_dotenv()
enctoken = os.getenv("ENCTOKEN")
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
    # print(df)
    time.sleep(2)

    print("\nTime:", datetime.datetime.now().time())
    adv_dec_df  = df.copy()
    adv_dec_df = adv_dec_df[adv_dec_df["segment"]!="INDICES"]
    advanced = 0
    declined = 0
    unchanged = 0
    for i in adv_dec_df.index:
        if adv_dec_df["ChangePrev%"][i] > 0:
            advanced+=1
        elif adv_dec_df["ChangePrev%"][i] < 0:
            declined+=1
        else:
            unchanged+=1
    print('Statics-------------------------------------------------------------\n',"Advanced - ",advanced," Declined - ",declined," & Unchanged - ",unchanged)

    # Index stock
    index_df = df.copy()
    index_df = index_df[index_df["segment"]=="INDICES"].reset_index()
    index_df = index_df[["tradingsymbol","LTP","ChangePrev%","instrument_token"]]
    print('\nIndex -------------------------------------------------------------\n',index_df)

    # Gainers stocks
    gainers_df = df.copy()
    gainers_df = gainers_df[gainers_df["segment"]!="INDICES"].reset_index()
    gainers_df = gainers_df.sort_values(by="ChangePrev%",ascending=False).reset_index(drop=True)
    gainers_df = gainers_df[["tradingsymbol","LTP","ChangePrev%","Volume","instrument_token"]]
    gainers_df = gainers_df[:5]
    print('\nGainers -------------------------------------------------------------\n',gainers_df)

    # Losers stocks
    losers_df = df.copy()
    losers_df = losers_df[losers_df["segment"]!="INDICES"].reset_index()
    losers_df = losers_df.sort_values(by="ChangePrev%",ascending=True).reset_index(drop=True)
    losers_df = losers_df[["tradingsymbol","LTP","ChangePrev%","Volume","instrument_token"]]
    losers_df = losers_df[:5]
    print('\nLosers -------------------------------------------------------------\n',losers_df)

    # Most active value stocks
    most_active_value_df = df.copy()
    most_active_value_df = most_active_value_df[most_active_value_df["segment"]!="INDICES"].reset_index()
    most_active_value_df = most_active_value_df.sort_values(by="TradedValue",ascending=False).reset_index(drop=True)
    most_active_value_df = most_active_value_df[["tradingsymbol","LTP","ChangePrev%","TradedValue","instrument_token"]]
    most_active_value_df = most_active_value_df[:5]
    print('\nMost active value -------------------------------------------------------------\n',most_active_value_df)

    # Most active volume stocks
    most_active_volume_df = df.copy()
    most_active_volume_df = most_active_volume_df[most_active_volume_df["segment"]!="INDICES"].reset_index()
    most_active_volume_df = most_active_volume_df.sort_values(by="Volume",ascending=False).reset_index(drop=True)
    most_active_volume_df = most_active_volume_df[["tradingsymbol","LTP","ChangePrev%","Volume","instrument_token"]]
    most_active_volume_df = most_active_volume_df[:5]
    print('\nMost active volume -------------------------------------------------------------\n',most_active_volume_df)

    exit()
    # time.sleep(1)