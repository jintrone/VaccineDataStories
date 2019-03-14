from bs4 import BeautifulSoup
import sys, os, glob, re, requests, time, datetime, pymongo
from sqlalchemy import *

from urllib.parse import urlparse



URLS = ["https://www.mothering.com/forum/47-vaccinations/",
        "https://www.mothering.com/forum/18041-vaccination-policies-legislation/",
        "https://www.mothering.com/forum/373-selective-delayed-vaccination/",
        "https://www.mothering.com/forum/17507-vaccinating-schedule/"]

ARCHIVE_URL = ["https://www.mothering.com/forum/69-vaccinations-archives/"]

def init():
    global engine, conn, metadata, mongodb
    engine = create_engine('mysql://ats:ats@localhost/ATS2')
    conn = engine.connect()
    metadata = MetaData(conn)
    metadata.reflect()
    client = pymongo.MongoClient()
    mongodb = client.mothering

def get_soup(url):
    print(f"Scraping {url}")
    txt = requests.get(url,headers={"User-Agent":"curl/7.54.0"}).text
    return (BeautifulSoup(txt,'html.parser'),txt)

def scrape_forum_index(url,start=1,last=None):
    soup = get_soup(url)[0]
    if not last:
        last = get_last_page_index(soup) + 1
    print(f"Will stop on index page {last-1}")


    for idx in range(start,last):
        curr_url = url
        retry_count = 0
        if idx > 1:
            while retry_count<4:
                try:
                    curr_url = f"{url}index{idx}.html"
                    soup = get_soup(curr_url)[0]
                    break
                except:
                    seconds = 10**retry_count
                    print(f"Presume we were rate limited; pausing for {seconds} seconds")
                    time.sleep(seconds)
                    retry_count+=1
                    print(f"Retry {retry_count}")


        urls=get_threads(soup)
        with open("mothering_threads.txt","a+") as f:
            f.write("\n".join(["\n# "+curr_url]+urls))

def build_thread_url(base_url,idx):
    """
    Build a thread index url given the base url of the thread and an index value

    :param base_url: The index of the thread
    :param idx: The index we want
    :return: The constructed url
    """
    matcher = re.compile("(.+/[^/.]+)\.html").match(base_url)
    return(f"{matcher.group(1)}-{idx}.html")


def slurp_raw_threads_to_mongo(url,start=1,last=None):
    """
    Just scrape all of the raw html to a mongo database for now; we will parse these out later

    :param url: The thread index url
    :param start: The first index page (optional)
    :param last: The last index page (optional - we'll figure this out if not provided)
    :return: Nothing
    """

    soup, text = get_soup(url)
    client = pymongo.MongoClient()
    db = client.mothering
    data = {"thread":url,"pages":[text]}
    with open("mothering_thread_log.txt","a+") as f:
        f.write(f"\n#############\n{url}")
    print(last)
    if not last:
        last = get_last_page_thread(soup) + 1
        print(f"After getting index: {last}")
    print(f"Will stop on index page {last-1}")
    for idx in range(start,last):

        curr_url = build_thread_url(url,idx)
        retry_count = 0

        # Here, we're doing some very naive stuff to back off the server if
        # our requests are timing out.  We're only going to try this 4 times
        # with increasing delays.
        if idx > 1:
            while retry_count<4:
                try:
                    soup, text = get_soup(curr_url)
                    data['pages'].append(text)
                    with open("mothering_post_pages.txt","a+") as f:
                        f.write(f"\n{curr_url}")
                    break
                except:
                    seconds = 10**retry_count
                    print(f"Presume we were rate limited; pausing for {seconds} seconds")
                    time.sleep(seconds)
                    retry_count+=1
                    print(f"Retry {retry_count}")

    db.pages.insert_one(data)
    if retry_count > 3:
        print("Too many retries...")
        sys.exit(-1)



def get_user_info_from_link(link_elt):
    if "members" in link_elt['href']:
        userpatt = re.compile(r"([^.]+)\.html")
        userinfo = {}
        userinfo["name"]=link_elt.string
        m = userpatt.match(link_elt['href'].split("/")[-1])
        userinfo["site_id"] = m.group(1)
        return userinfo, 1
    else:
        otherspatt = re.compile(r"(\d+)\sothers")
        m = otherspatt.match(link_elt.string)
    return None, int(m.group(1))

def get_post_id_from_link(link_elt):
    #print(link_elt)
    userpatt = re.compile(r"[^#]+#(post\d+)")
    return userpatt.match(link_elt).group(1)


def scrape_forum_posts(soup,store_date,page):
    #sel="html.gecko.mac.js body div.container div.content div#page.page.page-background div div#wrapper.wrapper div#main-content_wrapper.center div#main-content div#posts div#edit19272858 section#post19272858.tborder.vbseo_like_postbit.user-post"
    posts = []
    sel = "section.tborder.vbseo_like_postbit.user-post"
    for x in soup.select(sel):
        #print(x)
        postinfo = {}
        postinfo["site_id"]=x['id']
        postinfo["page"] = page
        id_num = re.match(r"post(\d+)",postinfo["site_id"]).group(1)
        content = x.select(f"div#post_message_{id_num}")[0]
        postinfo["raw_content"] = str(content)
        postinfo["clean_content"] = "\n".join(content.stripped_strings)
        #print(x.select("span.post-count>a"))
        postinfo["link"]=x.select("span.post-count>a")[0]["href"]
        datestring = x.find(itemprop="dateCreated").string
        if datestring.find("Today") > -1:

            creation_date = datetime.datetime.strptime(datestring,"Today, %I:%M %p").\
                replace(year=store_date.year,month=store_date.month,day=store_date.day)
            postinfo["created"] = creation_date
        elif datestring.find("Yesterday") > -1:
            creation_date = datetime.datetime.strptime(datestring,"Yesterday, %I:%M %p"). \
                replace(year=store_date.year,month=store_date.month,day=store_date.day)
            postinfo["created"] = creation_date

        else:
            postinfo["created"]= datetime.datetime.strptime(datestring,"%m-%d-%Y, %I:%M %p")
        postinfo["creator"] = get_user_info_from_link(x.select("a.bigusername")[0])[0]
        postinfo["likes"] = []
        postinfo["like_count"] = 0
        for l in x.select("div.alt2.vbseo_liked > a"):
            uinfo = get_user_info_from_link(l)
            if uinfo[0]:
                postinfo["likes"].append(uinfo[0])
                postinfo["like_count"]+=1
            else:
                postinfo["like_count"]+=uinfo[1]

        postinfo["quotes"] = []
        for q in x.select("div.quote_box"):

            quote = {}
            quote["raw_content"] = str(q)

            quote["clean_content"] = "\n".join(q.stripped_strings)
            #print(quote["clean_content"])
            reply_to = q.select("img.inlineimg[alt='View Post']")
            if reply_to:
                quote["site_id"] = get_post_id_from_link(reply_to[0].parent['href'])
            postinfo["quotes"].append(quote)

        posts.append(postinfo)
        #print(postinfo)
    return(posts)

def get_or_insert_mothering_poster(name,site_id,context):
    nid = context["posters"].get(site_id)
    if not nid:
        posters = metadata.tables["posters"]
        ins = posters.insert()
        nid = conn.execute(ins,username=name, name=name,site_id=site_id,profile_domain="mothering.com",anonymous=0).inserted_primary_key[0]
        context['posters'][site_id] = nid
    return(nid)

def insert_new_post(post,creator,thread_id,context):
    posts = metadata.tables["content"]
    ins = posts.insert()
    nid = conn.execute(ins,site_id = post['site_id'],likes = post['like_count'],
                       thread = thread_id, creator=creator,raw = post['raw_content'],
                       clean = post["clean_content"],link=post["link"],
                       creation_date = post["created"],dislikes = 0, page = post["page"]).inserted_primary_key[0]
    context["posts"][post['site_id']] = nid
    return(nid)



def insert_thread(thread,date):
    threads = metadata.tables["threads"]
    id = re.match(r".+/(\d+)-.+\.html",thread).group(1)
    nid = conn.execute(threads.insert(),site_id = id, domain="mothering.com", link=thread,last_scraped=date).inserted_primary_key
    return(nid)

def insert_quotes(quote_map):
    quotes_table = metadata.tables["quotes"]
    data = []
    for post_id, quotes in quote_map.items():
        data += [ {"src_id":post_id, "targ_id":x.get("targ_id"),
              "raw":x.get("raw_content"), "clean":x.get("clean_content")} for x in quotes]
    conn.execute(quotes_table.insert(),data)

def insert_likes(likes_map):
    relations = metadata.tables["relations"]
    data = []
    for post_id, likes in likes_map.items():
        data += [ {"src_user":x, "targ_id":post_id, "type":"like"} for x in likes]
    l = relations.insert()
    conn.execute(l,data)


def insert_mothering_posts(posts,context,thread_id):
    if not "posters" in context:
        context["posters"] = {}
        context["posts"] = {}
        context["quotes"] = {}
        context["likes"] = {}

    for p in posts:
        creator_id = get_or_insert_mothering_poster(p['creator']['name'],p['creator']['site_id'],context)
        post_id = insert_new_post(p,creator_id,thread_id,context)
        liking_posters = []
        for l in p['likes']:
            lp = get_or_insert_mothering_poster(l['name'],l['site_id'],context)

            liking_posters.append(lp)
        if liking_posters:
            context["likes"][post_id] = liking_posters
        if p["quotes"]:
            for q in p["quotes"]:
                q["targ_id"] = context["posts"].get(q.get("site_id"))

            context["quotes"][post_id] = p["quotes"]


def get_threads(soup):
    sel = "table#threadslist.tborder.vs_showthreads td.alt1 div a.thread_title_link"
    return [x['href'] for x in soup.select(sel) if x.previous_element.string.lower().find("sticky")==-1]


def get_last_page_index(soup):
    #sel = "div#main-content div.fixed-controls-container div.pagenav table.tborder tbody tr td.alt1:nth-last-child(2) a.smallfont"
    sel = "div#main-content div.fixed-controls-container div.pagenav td.alt1 a.smallfont"
    url = soup.select(sel)[-1]["href"]
    if url:
        p = re.compile(r"index(\d*)\.html")
        return int(p.search(url).group(1))
    else:
        return 1


def get_last_page_thread(soup):
    #sel = "div#main-content div.fixed-controls-container div.pagenav table.tborder tbody tr td.alt1:nth-last-child(2) a.smallfont"
    sel = "div#main-content div.fixed-controls-container div.pagenav td.alt1 a.smallfont"
    urls = soup.select(sel)
    print(urls)
    if urls:
        p = re.compile(r"/[^.]+-(\d*)\.html")
        return int(p.search(urls[-1]["href"]).group(1))
    else:
        return 1



def test_file():
    with open("mothering_index.html") as f:
        txt = f.read()
    #print(txt)
    soup = BeautifulSoup(txt, 'html.parser')
    print("\n".join(get_threads(soup)))


def test_index_scraper():
    global URLS
    scrape_forum_index(URLS[3])

def slurp_known_threads():
    with open("mothering_threads.txt") as f:
        stopping = "https://www.mothering.com/forum/47-vaccinations/395923-feingold-diet.html"
        read = False
        for line in f:
            if read:
                if not line.startswith("#"):
                    slurp_raw_threads_to_mongo(line.strip())
            else:
                if stopping in line:
                    print("Found stopping point")
                    read = True



def test_thread_scraper():
    url = "https://www.mothering.com/forum/47-vaccinations/1513297-how-dismiss-vaccine-reaction-seven-easy-steps.html"
    slurp_raw_threads_to_mongo(url)
    # test_url = "https://www.mothering.com/forum/47-vaccinations/1610489-misogyny-anti-vax-quote.htm1"
    # scrape_threads(test_url)



def xfer_mongo_to_db():
    init()
    client = pymongo.MongoClient()
    db = client.mothering
    context = {}
    count = 0
    for thread in db.pages.find({}):
        count+=1
        thread_id = insert_thread(thread["thread"],thread["_id"].generation_time.date())
        #print("Pages",len(thread['pages']))
        for page in zip(range(len(thread['pages'])),thread['pages']):
            soup = BeautifulSoup(page[1],'lxml')
            posts = scrape_forum_posts(soup,thread["_id"].generation_time.date(),page[0])
            insert_mothering_posts(posts,context,thread_id)
        if count%500 == 0:
            print(f"Scraped {count}")
    print("Updating likes...")
    insert_likes(context["likes"])
    print("Updating quotes...")
    insert_quotes(context["quotes"])




def test_post_scraper():
    client = pymongo.MongoClient()
    db = client.mothering

    page = db.pages.find_one()
    soup = BeautifulSoup(page['pages'][0],'lxml')
    posts = scrape_forum_posts(soup,page["_id"].generation_time.date(),1)

    #BeautifulSoup(txt,'html.parser')



    # with open("mothering_posts.html") as f:
    #     txt = f.read()
    #     soup = BeautifulSoup(txt, 'html.parser')
    #     scrape_forum_posts(soup)
