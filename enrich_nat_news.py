import json, requests
import pymongo
from pymongo import ReplaceOne
import sys
from urllib.parse import urlparse
from sqlalchemy import *
import datetime, time, os


API_KEY=None
get_posts = "https://disqus.com/api/3.0/threads/listPosts.json"

metadata = None
engine = None
conn = None
mongodb = None
start_time = None
request_count = 0


def init():
    global engine, conn, metadata, mongodb, API_KEY
    #engine = create_engine('mysql://ats:ats@localhost/ATS2')
    #conn = engine.connect()
    #metadata = MetaData(conn)
    #metadata.reflect()
    client = pymongo.MongoClient()
    mongodb = client.natnews
    with open(f"local_config_{os.environ['LOGNAME']}.json") as f:
        API_KEY = json.load(f)['disqus_api']


def get_posts_data(thread_id):
    global start_time, request_count
    next = None
    start = True
    posts = []
    while start or next:
        if (datetime.datetime.now()-start_time).total_seconds() < 3600 and request_count >= 999:
            tosleep = 3600 - (datetime.datetime.now()-start_time).total_seconds()
            print(f"Sleeping till quota reset at about {start_time+datetime.timedelta(seconds=int(tosleep))}")
            time.sleep(tosleep)
            print("Waking up")
            start_time = datetime.datetime.now()
            request_count = 0


        start = False
        params = {"api_key":API_KEY,"thread":thread_id,"limit":100}
        if next:
            params["cursor"] = next
        result = requests.get(get_posts,params).json()
        request_count+=1
        #print(result)
        if result["cursor"]["hasNext"]:
            next = result["cursor"]["next"]
        else:
            next = None
        posts = posts+result["response"]
    return posts


def update_threads():
    global start_time
    start_time = datetime.datetime.now()
    if not mongodb:
        init()
    pipeline = [{"$group":{"_id": "$thread", "count":{"$sum":1}}},{"$match":{"count":{"$gt":49}}}]
    result = list(mongodb.comments.aggregate(pipeline))
    with open("thread_log.json","w") as f:
        json.dump(result,f)
    print(f"Will attempt to enrich {len(result)} threads")
    for ent in result:
        thread_id = ent['_id']
        print(f"Thread {thread_id}")
        posts = get_posts_data(thread_id)
        bulkreplace = []
        for p in posts:
            bulkreplace.append(ReplaceOne({"id":p["id"]},p,True))
        #print(json.dumps(bulkreplace))
        #x = input("Proceed? ")
        #if x == "n":
        #    sys.exit(-1)
        mongodb.comments.bulk_write(bulkreplace)
