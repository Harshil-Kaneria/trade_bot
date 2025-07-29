import os
from dotenv import load_dotenv
import hashlib
import requests
from api_helper import NorenApiPy
import logging

logging.basicConfig(level=logging.DEBUG)
load_dotenv(override=True)
enctoken = os.getenv("ENCTOKEN")

userid = os.getenv("FLATTRADE_USER_ID")
api_key = os.getenv("FLATTRADE_API_KEY")
api_secret = os.getenv("FLATTRADE_SECRET_KEY")
request_token = os.getenv("FLATTRADE_CODE")
usertoken = os.getenv("FLATTRADE_SESSION")

def generate_hash(api_key, request_token, api_secret):
    return hashlib.sha256(f"{api_key}{request_token}{api_secret}".encode()).hexdigest()

def get_api_token(payload):
    try:
        response = requests.post("https://authapi.flattrade.in/trade/apitoken", json=payload)
        return response.json() if response.status_code == 200 else {"error": f"HTTP {response.status_code}", "details": response.text}
    except requests.exceptions.RequestException as e:
        return {"error": "RequestException", "details": str(e)}

if usertoken=='':
    hashed_secret = generate_hash(api_key, request_token, api_secret)
    output = {"api_key": api_key, "request_code": request_token, "api_secret": hashed_secret}
    response_data = get_api_token(output)
    if response_data.get("error") is None:
        print("response_data",response_data)
        print("usersession",response_data['token'])
else:
    api = NorenApiPy()
    ret = api.set_session(userid= userid, password = '', usertoken= usertoken)
    ret = api.get_limits()
    print(ret)