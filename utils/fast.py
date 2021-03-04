
import fastapi
import urllib
from fastapi.middleware.cors import CORSMiddleware
from typing import Dict

def enable_cors(app:fastapi.FastAPI):
	app.add_middleware(CORSMiddleware,
    	allow_origins = ["*"],
        allow_methods = ["*"],
        allow_headers = ["*"],
        allow_credentials = True)
