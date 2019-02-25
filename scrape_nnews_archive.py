from bs4 import BeautifulSoup
import sys, os, glob
from urllib.parse import urlparse
import requests, time, random

SUPPORTED_URLS = ["www.naturalnews.com"]

def nth_sibling(elt,index):
    idx = 0
    for sib in elt.next_siblings:
        if idx == index:
            return sib
        idx+=1
    return None


def scrape_text(txt):
    soup = BeautifulSoup(txt, 'html.parser')
    results = []
    for match in soup.select("table:nth-of-type(3) tr td a"):
        if not match.has_attr("href"):
            continue
        test = repr(match)
        if (test is None):
            test = ""
        sib = nth_sibling(match,4)
        if sib:
            test+=repr(sib)
        if "vaccin" in test.lower():
            results.append(match['href'])
    return results

def get_html_files(path):
    return(glob.glob(f'{path}/*.html'))


def scrape_index(start = 1, end=721,output="../disqus-crawler/urls.txt"):
    with open(output,"w") as out:
        for page in range(start,end+1):
            url = f"https://www.naturalnews.com/index_1_1_{page}.html"
            print(f"Scraping {url}")
            text = requests.get(url).text
            urls = scrape_text(text)
            if urls:
                out.write("\n".join(urls)+"\n")

            time.sleep(1+random.randint(1,4))


if __name__ == "__main__":
    #with open("test.html",encoding="utf-8") as f:
    #    print(scrape_text(f.read()))
    scrape_index(start=1,end=42)