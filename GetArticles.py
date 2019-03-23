"""
It adds the articles to the table
"""

import requests
from DBModels import Threads, Articles 
from dbConnect import DBConnect
from bs4 import BeautifulSoup
import pandas as pd

def insert_threads_to_articles_bulk(batch_size_limit):
    """
    Summary
    --------
    This function reads the table of Threads and inserts threads.id
    and threads.link to Articles table for thread_id and link correspondingly

    Parameters
    -----------
    batch_size_limit : int
    
    Returns 
    -------
    None
    """
    batch_size = 0
    batch_data = []
    total  = 0
    
    # check for already inserted one
    existing_articles = set([articles.link for articles in session.query(Articles).all()])
    existing_count = len(existing_articles)
    existing_found = 0
    
    for threads in session.query(Threads).all():
        if batch_size == batch_size_limit:
            session.bulk_insert_mappings(Articles, batch_data)
            # reset size and batch data
            batch_size = 0
            batch_data = []
            session.commit()  
            print('Inserted {0} records'.format(batch_size_limit))

        scrape_link = threads.link
        # check if these link are already existing then no need to look for it
        if scrape_link not in existing_articles:
            tmp_batch_data = get_articles_content(threads.id, scrape_link)
            # if no error then add to the batch
            if tmp_batch_data: 
                batch_data.append(tmp_batch_data)
                batch_size = batch_size + 1
                total = total + 1
        else:
            # counting the existing
            existing_found = existing_found + 1
            print('Existing: ', existing_found)
            
    print('Existing {0} of {1}'.format(existing_found, existing_count))

    session.bulk_insert_mappings(Articles, batch_data)
    session.commit() 
    print('Inserted total - {0} records'.format(total))


def get_articles_content(thread_id, url_read):
    """
    Summary
    --------
    This function is helper function for getting the articles and cleaning them and returning the 
    dictionary format that can be used as Articles model
    
    Parameters
    ----------
    thread_id: int
        thread_id from Threads
    url_read: string
        url to read for the article
        
    Returns
    -------
    If no error is occurred
        dictionary that can be used as Article 
    Else catch exception keep url and return None
    """
    
    global error_urls
    
    r = requests.get(url_read)
    if r.status_code == requests.codes.ok:
        print(url_read)
        print('--------------------------------------------------------------')  
        try:
            soup = BeautifulSoup(r.text, 'html.parser')
            article_title = soup.select('#ArticleCol1 h1')[0].get_text()
            title_txt = article_title
        
            article_raw_text = soup.select('#ArticleCol1 #Article')
            
            if len(article_raw_text) == 0:
                article_raw_text = soup.select('#ArticleCol1 .entry-content')

            remove_block = article_raw_text[0].select('.SocialBlock')
            if remove_block:
                remove_block[0].decompose()
        

            for t in article_raw_text[0].find_all('br'):
                t.replace_with('\n')
                
            clean_txt = article_raw_text[0].get_text().strip()
            
            return dict(threads_id=thread_id,
                        link=url_read,
                        raw_content=r.text,
                        clean_content=clean_txt,
                        title=title_txt)
        except:
            error_urls.append(url_read)

            return dict(threads_id=thread_id,
                        link=url_read,
                        raw_content=r.text,
                        clean_content='',
                        title='')
            
    else:
        print('Error in request!!!')
        r.raise_for_status()


if __name__ == '__main__':
    config_file = "configuration.json"
    error_urls = []

    # get connection engine and session
    db_connect = DBConnect()
    db_connect.start_db(config_file)
    engine = db_connect.get_engine()
    session = db_connect.get_session()
    
    insert_threads_to_articles_bulk(30)
    
    # create csv for all errored url
    error_urls_df = pd.DataFrame({'urls' : error_urls})
    error_urls_df.to_csv('error_url.csv', index=False)