"""
It adds the articles to the table
"""

from bs4 import BeautifulSoup
from DBModels import Threads, Articles 
from dbConnect import DBConnect

import pandas as pd

global session
            
def clean_articles():
    error_df = pd.read_csv('error_url.csv')
    
    error_urls = error_df['urls'].values.tolist()
    
    for article in session.query(Articles).all():
        
        if article.link in error_urls:
            soup = BeautifulSoup(article.raw_content, 'html.parser')
            article_title = soup.select('head > title')[0].get_text()
            article.title = article_title
            
            article_raw_text = soup.select('#Article')
                    
            remove_block = article_raw_text[0].select('.SocialBlock')
            if remove_block:
                remove_block[0].decompose()
            
    
            for t in article_raw_text[0].find_all('br'):
                t.replace_with('\n')
                    
            article.clean_content = article_raw_text[0].get_text().strip()
            session.commit()
            
def update_threads_table():
    session.query(Threads).filter(Threads.id==Articles.threads_id).update({Threads.article:Articles.id}, synchronize_session='fetch')
    session.commit()

if __name__ == '__main__':
    config_file = "configuration.json"
    global session

    # get connection engine and session
    db_connect = DBConnect()
    db_connect.start_db(config_file)
    engine = db_connect.get_engine()
    session = db_connect.get_session()
    clean_articles()
    update_threads_table()
