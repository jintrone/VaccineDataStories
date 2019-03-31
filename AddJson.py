"""
It adds the json to the table
"""

# from bs4 import BeautifulSoup
from DBModels import JsonData
from dbConnect import DBConnect

import pandas as pd
import json

global session

def add_json(json_file):
    with open(json_file) as f:
        json_input = json.load(f)

    for i in json_input:
        d = JsonData(id = i['thread_id'], json_data = i)
        session.add(d)
        session.commit()

if __name__ == '__main__':
    config_file = "configuration.json"
    json_file = "json_outputs.json"
    global session

    # get connection engine and session
    db_connect = DBConnect()
    db_connect.start_db(config_file)
    engine = db_connect.get_engine()
    session = db_connect.get_session()
    add_json(json_file)
