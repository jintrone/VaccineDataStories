from bs4 import BeautifulSoup
import sys, os, glob, re, requests, time, datetime
from urllib.parse import urlparse



URLS = ["https://www.mothering.com/forum/47-vaccinations/",
        "https://www.mothering.com/forum/18041-vaccination-policies-legislation/",
        "https://www.mothering.com/forum/373-selective-delayed-vaccination/",
        "https://www.mothering.com/forum/17507-vaccinating-schedule/"]

ARCHIVE_URL = ["https://www.mothering.com/forum/69-vaccinations-archives/"]

def get_soup(url):
    print(f"Scraping {url}")
    txt = requests.get(url,headers={"User-Agent":"curl/7.54.0"}).text
    return BeautifulSoup(txt,'html.parser')

def scrape_forum_index(url,start=1,last=None):
    soup = get_soup(url)
    if not last:
        last = get_last_index(soup)+1
    print(f"Will stop on index page {last-1}")


    for idx in range(start,last):
        curr_url = url
        retry_count = 0
        if idx > 1:
            while retry_count<4:
                try:
                    curr_url = f"{url}index{idx}.html"
                    soup = get_soup(curr_url)
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


def get_user_info_from_link(link_elt):
    userpatt = re.compile(r"([^.]+)\.html")
    userinfo = {}
    userinfo["name"]=link_elt.string
    m = userpatt.match(link_elt['href'].split("/")[-1])
    userinfo["site_id"] = m.group(1)
    return userinfo


def scrape_forum_posts(soup):
    #sel="html.gecko.mac.js body div.container div.content div#page.page.page-background div div#wrapper.wrapper div#main-content_wrapper.center div#main-content div#posts div#edit19272858 section#post19272858.tborder.vbseo_like_postbit.user-post"
    sel = "section.tborder.vbseo_like_postbit.user-post"

    for x in soup.select(sel):
        #print(x)
        postinfo = {}
        postinfo["site_id"]=x['id']
        postinfo["link"]=x.select("span.postcount>a")["href"]
        postinfo["created"]= datetime.datetime.strptime(x.find(itemprop="dateCreated").string,"%m-%d-%Y, %I:%M %p")
        userinfo = get_user_info_from_link(x.select("a.bigusername")[0])
        likes = [get_user_info_from_link(l) for l in x.select("div.alt2.vbseo_liked > a")]



def get_threads(soup):
    sel = "table#threadslist.tborder.vs_showthreads td.alt1 div a.thread_title_link"
    return [x['href'] for x in soup.select(sel) if x.previous_element.string.lower().find("sticky")==-1]


def get_last_index(soup):
    #sel = "div#main-content div.fixed-controls-container div.pagenav table.tborder tbody tr td.alt1:nth-last-child(2) a.smallfont"
    sel = "div#main-content div.fixed-controls-container div.pagenav td.alt1 a.smallfont"
    url = soup.select(sel)[-1]["href"]
    p = re.compile(r"index(\d*)\.html")
    return int(p.search(url).group(1))



def test_file():
    with open("mothering_index.html") as f:
        txt = f.read()
    #print(txt)
    soup = BeautifulSoup(txt, 'html.parser')
    print("\n".join(get_threads(soup)))


def test_index_scraper():
    global URLS
    scrape_forum_index(URLS[3])

def test_post_scraper():
    with open("mothering_posts.html") as f:
        txt = f.read()
        soup = BeautifulSoup(txt, 'html.parser')
        scrape_forum_posts(soup)
