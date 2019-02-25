import json
import pymongo
import sys
from urllib.parse import urlparse
from sqlalchemy import *
import datetime


metadata = None
engine = None
conn = None
mongodb = None

def init():
    global engine, conn, metadata, mongodb
    engine = create_engine('mysql://ats:ats@localhost/ATS2')
    conn = engine.connect()
    metadata = MetaData(conn)
    metadata.reflect()
    #print(metadata.is_bound())
    #print(metadata.tables)
    client = pymongo.MongoClient()
    mongodb = client.natnews

def batch_insert_user(data):
    posters = metadata.tables["posters"]
    conn.execute(posters.insert(),data)

def batch_insert_thread(data):
    threads = metadata.tables["threads"]
    conn.execute(threads.insert(),data)

def batch_insert_posts(data):
    posts = metadata.tables["content"]
    conn.execute(posts.insert(),data)

def batch_insert_replies(data):
    replies = metadata.tables["replies"]
    conn.execute(replies.insert(),data)


def get_or_insert_thread(conn,url,site_id):
    thread = metadata.tables["threads"]
    s = select[thread.c.id].where(thread.c.site_id == id and thread.c.link == url)
    result = conn.execute(s)
    for row in result:
        return row[0]
    i = thread.insert().values(link=url,site_id=id)
    result = conn.execute(i)
    return result.inserted_primary_key

def get_or_insert_comment(conn,site_id,thread_id,creator_id,raw,link,creation_date):
    content = metadata.tables["content"]
    #s = select[content.c.id].where(content.c.thread==thread_id and )


def xfer_users():
    users = []
    user_table = metadata.tables["posters"]
    anon_names = set([])
    members = set([])
    nm_count = 0
    na_count = 0
    nm_skip = 0
    na_skip = 0
    s = select([user_table.c.site_id,user_table.c.name,user_table.c.anonymous])
    for u in conn.execute(s):
        if u[2]:
            anon_names.add(u[1].strip().lower())
        else:
            members.add(u[0])
    for author in mongodb.comments.distinct("author"):
        anon = author.get('isAnonymous')
        if anon:
            name = author.get("name").strip().lower()
            if name in anon_names:
                na_skip+=1
                continue
            na_count +=1
        else:
            if author.get("id") in members:
                nm_skip+=1
                continue
            else:
                name = author.get("name")
            nm_count+=1
        users.append({"username":author.get("username"),
                      "name":name,
                      "site_id":author.get("id"),
                      "profile_url":author.get("profileUrl"),
                      "profile_domain":urlparse(author["profileUrl"]).netloc,
                      "anonymous": 1 if anon else 0}
                     )
    if input(f"Would insert {len(users)} (anon: skipped-{na_skip} retained-{na_count}, members: skipped-{nm_skip} retained-{nm_count}). Continue?")=="n":
        sys.exit(0)
    batch_insert_user(users)

def xfer_threads():
    threads = []
    pipeline = [{ "$group": { "_id": {"thread_id": "$thread", "url": "$blog_url"} } }]
    for thread in mongodb.comments.aggregate(pipeline):
        thread = thread["_id"]
        threads.append({ "site_id":thread.get("thread_id"),
                         "domain":"disqus.com",
                         "link":thread.get("url"),
                         "last_scraped":datetime.datetime.now()})
    batch_insert_thread(threads)


def xfer_posts(threads = None):
    thread_map = {}
    threads_table = metadata.tables["threads"]
    posts_table = metadata.tables["content"]
    s = select([threads_table.c.site_id, threads_table.c.id])

    posts_total = 0
    posts_skipped = 0

    for row in conn.execute(s):
        thread_map[row[0]] = row[1]

    users_map = {}
    anon_map = {}
    users = metadata.tables["posters"]
    s = select([users.c.id,users.c.site_id,users.c.name])
    print("Building user map...")
    for row in conn.execute(s):
        if row[1]:
            users_map[row[1]] = row[0]
        else:
            anon_map[row[2].strip().lower()] = row[0]
    posts = []


    threads_to_update = set(thread_map.keys())

    if threads:
        threads_to_update = set(threads)
    print(f"Have {len(threads_to_update)}")
    for t in threads_to_update:
        s = select([posts_table.c.site_id]).where(posts_table.c.thread == thread_map[t])
        existing_post_ids = set([r[0] for r in conn.execute(s)])
        for post in mongodb.comments.find({"thread":t}):
            posts_total+=1
            if post["id"] in existing_post_ids:
                posts_skipped+=1
                continue
            author_name = post["author"]["name"]
            if post["author"]["isAnonymous"]:
                author_name = author_name.lower()
                if author_name=="ä":
                    author_name="a"
                creator_fk = anon_map[author_name.strip()]
            else:
                #print("not anonymous")
                creator_fk = users_map[post["author"]["id"]]


            thread_fk = thread_map[post["thread"]]

            parsed_date = datetime.datetime.strptime(post["createdAt"],"%Y-%m-%dT%H:%M:%S")
            #input("keep going")

            posts.append({"site_id":post.get("id"),
                          "creator":creator_fk,
                          "thread":thread_fk,
                          "raw":post.get("message"),
                          "clean":post.get("raw_message"),
                          "link":None,
                          "page":None,
                          "likes":post.get("likes"),
                          "dislikes":post.get("dislikes"),
                          "creation_date":parsed_date
                          })

    print(type(posts))
    cont = input(f"Would insert {len(posts)} of {posts_total} (difference should be {posts_skipped})")
    if cont=="n":
        sys.exit(0)
    batch_insert_posts(posts)


def xfer_posts_fast(threads = None):
    thread_map = {}
    threads_table = metadata.tables["threads"]
    posts_table = metadata.tables["content"]
    s = select([threads_table.c.site_id, threads_table.c.id])

    posts_total = 0
    posts_skipped = 0

    for row in conn.execute(s):
        thread_map[row[0]] = row[1]

    users_map = {}
    anon_map = {}
    users = metadata.tables["posters"]
    s = select([users.c.id,users.c.site_id,users.c.name])
    print("Building user map...")
    for row in conn.execute(s):
        if row[1]:
            users_map[row[1]] = row[0]
        else:
            anon_map[row[2].strip().lower()] = row[0]
    posts = []



    for post in mongodb.comments.find({}):
        author_name = post["author"]["name"]
        if post["author"]["isAnonymous"]:
            author_name = author_name.lower()
            if author_name=="ä":
                author_name="a"
            creator_fk = anon_map[author_name.strip()]
        else:
            #print("not anonymous")
            creator_fk = users_map[post["author"]["id"]]


        thread_fk = thread_map[post["thread"]]

        parsed_date = datetime.datetime.strptime(post["createdAt"],"%Y-%m-%dT%H:%M:%S")
        #input("keep going")

        posts.append({"site_id":post.get("id"),
                      "creator":creator_fk,
                      "thread":thread_fk,
                      "raw":post.get("message"),
                      "clean":post.get("raw_message"),
                      "link":None,
                      "page":None,
                      "likes":post.get("likes"),
                      "dislikes":post.get("dislikes"),
                      "creation_date":parsed_date
                      })


    cont = input(f"Would insert {len(posts)} ?")
    if cont=="n":
        sys.exit(0)
    batch_insert_posts(posts)


def xfer_replies():
    replies = []
    post_map = {}
    posts_table = metadata.tables["content"]
    s = select([posts_table.c.site_id, posts_table.c.id])
    for p in conn.execute(s):
        post_map[p[0]]=p[1]
    for post in mongodb.comments.find({"parent": {"$ne":None}}):
        replies.append({ "src_id":post_map.get(post["id"]),
                         "targ_id":post_map.get(str(post["parent"])),
                         "targuser":None,
                         "type":"direct"})
    batch_insert_replies(replies)



def xfer_everything():
    #Get users
    init()
    #xfer_users()
    #xfer_threads()
    #with open("thread_log.json") as f:
    #    threads = [x['_id'] for x in json.load(f)]
    #xfer_posts(threads)
    #xfer_posts_fast()
    xfer_replies()







