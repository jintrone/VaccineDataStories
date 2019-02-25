from bs4 import BeautifulSoup
import sys, os, glob
from urllib.parse import urlparse


SUPPORTED_URLS = ["www.naturalnews.com"]

def scrape_text(txt):
    soup = BeautifulSoup(txt, 'html.parser')
    results = []
    for match in soup.select("div.g div.r>a"):
        results.append(match['href'])
    return results

def get_html_files(path):
    return(glob.glob(f'{path}/*.html'))

if __name__ == "__main__":
    files = []
    if len(sys.argv) > 1:
        if os.path.isdir(sys.argv[1]):
            files = get_html_files(sys.argv[1])
        else:
            files = sys.argv[1:]

    with open('../disqus-crawler/urls.txt',"w") as out:
        for f in files:
            print(f"Scraping {f}")
            if os.path.isfile(f):
                with open(f) as infile:
                    urls = scrape_text(infile.read())
                for u in urls:
                    if urlparse(u).netloc in SUPPORTED_URLS and u.endswith("html"):
                        out.write(f'{u}\n')



