import os
import json
from dotenv import load_dotenv
from kite_trade import *

load_dotenv(override=True)
enctoken = os.getenv("ENCTOKEN")
kite = KiteApp(enctoken=enctoken)
data = kite.instruments("NSE")

json_filename = "static/symbols.json"
with open(json_filename, "w") as json_file:
    json.dump(data, json_file, indent=4)

print(f"JSON file '{json_filename}' generated successfully.")