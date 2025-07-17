import os
import pyotp
from dotenv import load_dotenv

from NorenRestApiPy.NorenApi import  NorenApi
api = None
class ShoonyaApiPy(NorenApi):
    def __init__(self):
        NorenApi.__init__(self, host='https://api.shoonya.com/NorenWClientTP/', websocket='wss://api.shoonya.com/NorenWSTP/')        
        global api
        api = self

load_dotenv(override=True)

api = ShoonyaApiPy()

user    = os.getenv("SHOONYA_USER")
pwd     = os.getenv("SHOONYA_PWD")
factor2 = pyotp.TOTP(os.getenv("SHOONYA_FACTOR2"))
vc      = os.getenv("SHOONYA_VC")
app_key = os.getenv("SHOONYA_APP_KEY")
imei    = os.getenv("SHOONYA_IMEI")
ret = api.login(userid=user, password=pwd, twoFA=factor2, vendor_code=vc, api_secret=app_key, imei=imei)

print(f"ret {ret}")