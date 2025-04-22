import os
from dotenv import load_dotenv
import pandas as pd
from flask import Flask, jsonify
from kite_trade import *
from kiteconnect import KiteTicker

# Load environment variables
load_dotenv(override=True)
enctoken = os.getenv("ENCTOKEN")
kite = KiteApp(enctoken=enctoken)

app = Flask(__name__)

symbols_list = list(pd.read_csv('data_500.csv')['SYMBOL'])
df = pd.DataFrame(kite.instruments("NSE"))
df = df[df["tradingsymbol"].isin(symbols_list)].reset_index(drop=True)
df = df[df["segment"]!="INDICES"].reset_index()

@app.route("/api/instruments/nse", methods=["GET"])
def get_nse_instruments():
    try:
        return jsonify(df.to_dict(orient="records"))
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    app.run(debug=True)
