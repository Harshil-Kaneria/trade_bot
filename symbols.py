import os
import json
from dotenv import load_dotenv
from kite_trade import *
from datetime import date, datetime

# Load environment variables
load_dotenv(override=True)
enctoken = os.getenv("ENCTOKEN")

# Initialize KiteApp
kite = KiteApp(enctoken=enctoken)

# Fetch instrument data
data = kite.instruments("NSE")

# Function to convert date objects to string
def convert_dates(obj):
    if isinstance(obj, (date, datetime)):
        return obj.isoformat()  # Convert to string format
    raise TypeError(f"Object of type {obj.__class__.__name__} is not JSON serializable")

# JSON filename
json_filename = "static/symbols.json"

# Write data to JSON with date conversion
with open(json_filename, "w") as json_file:
    json.dump(data, json_file, indent=4, default=convert_dates)

print(f"JSON file '{json_filename}' generated successfully.")
