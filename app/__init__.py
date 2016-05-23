from flask import Flask
from monitor_local import query_periodically

app = Flask(__name__)
from app import views