from bs4 import BeautifulSoup
from DBModels import Threads, Articles, Content, Posters, Relations
from dbConnect import DBConnect
from collections import defaultdict
import pandas as pd
import numpy as np
import json

global session

posts_data_list = []
article_data_list = []

config_file = "configuration.json"

# get connection engine and session
db_connect = DBConnect()
db_connect.start_db(config_file)
engine = db_connect.get_engine()
session = db_connect.get_session()
    
def update(d, u):
    
    for item in d:
        try:
            if item['id'] == u['parent']:
                item['replies'].append(u)
                
            else:
                # if not do recursive check in children
                update(item['replies'], u)
                    
        except:
            print(item)          
            raise            
    return d   
            
relations_df = pd.read_sql(session.query(Relations).statement,session.bind) 
relations_df = relations_df[['id', 'src_id', 'targ_id']]
relations_df.rename(columns={'id':'relations_id'}, inplace=True)
relations_df.dropna(subset=['targ_id'], inplace=True)

relations_df['src_id'] = relations_df['src_id'].astype(int)
relations_df['targ_id'] = relations_df['targ_id'].astype(int)

# get contents 
content_df = pd.read_sql(session.query(Content).statement,session.bind) 
content_df = content_df[['id', 'site_id', 'thread', 'raw', 'creation_date', 'creator']]
    
# separate df to keep track of creation date
content_creation = content_df[['id', 'creation_date']]
    
content_df['creation_date'] = content_df['creation_date'].astype(str)
content_df['id'] = content_df['id'].astype(int)
content_df['thread'] = content_df['thread'].astype(int)

content_df.rename(columns={'id':'content_id', 'site_id':'content_site_id'}, inplace=True)

posters_df = pd.read_sql(session.query(Posters).statement,session.bind) 
posters_df = posters_df[['id', 'name', 'anonymous']]
posters_df.rename(columns={'id':'poster_id'}, inplace=True)
    
content_posters_df = content_df.merge(posters_df, how='left', left_on='creator', right_on='poster_id') 
# find the date of creations for each contents
relations_creation_df = relations_df.merge(content_creation, how='left', left_on='src_id', right_on='id')
    
relations_creation_df.sort_values('creation_date', ascending=True, inplace=True)

inserted_set = set()

for _, row in relations_creation_df.iterrows():
    
    # creating node
    post_map = defaultdict()
        
    post_content = content_posters_df[content_posters_df['content_id'] == row['src_id']]
    
    
    post_map['id'] = int(post_content.iloc[0]['content_id'])
    post_map['content'] = post_content.iloc[0]['raw']
    post_map['creation_date'] = post_content.iloc[0]['creation_date']
    post_map['name'] = post_content.iloc[0]['name']
    post_map['anonymous'] = int(post_content.iloc[0]['anonymous'])
    post_map['replies'] = []
    post_map['thread'] = int(post_content.iloc[0]['thread'])
    parent_post_id = int(row['targ_id'])
        
    post_map['parent'] = parent_post_id
    
    # if parent node is not in the dictionary
    if parent_post_id not in inserted_set:
        
        parent_post = defaultdict()
        parent_post['id'] = parent_post_id
        parent_post['replies'] = []
        parent_post['parent'] = 0

        parent = content_posters_df[content_posters_df['content_id']==parent_post_id]
        try:
            parent_post['content'] = parent.iloc[0]['raw']
            parent_post['creation_date'] = parent.iloc[0]['creation_date']
            parent_post['name'] = parent.iloc[0]['name']
            parent_post['anonymous'] = int(parent.iloc[0]['anonymous'])
            parent_post['thread'] = int(parent.iloc[0]['thread'])

            posts_data_list.append(parent_post)
            inserted_set.add(parent_post_id)
            
        except:
            print('Error')
            print('Id: ', parent_post_id)
            raise Exception('Parent not found!')
            
    
        parent_post['replies'].append(post_map)
        inserted_set.add(post_map['id'])
        
    else:
    
        posts_data_list = update(posts_data_list, post_map)
        inserted_set.add(post_map['id'])
  
# for those that are not in relations
for _, row in content_posters_df.iterrows():
    
    post_id = int(row['content_id'])
    
    if post_id not in inserted_set:
        post_map = defaultdict()
    
        post_map['id'] = int(row['content_id'])
        post_map['content'] = row['raw']
        post_map['creation_date'] = row['creation_date']
        post_map['name'] = row['name']
        post_map['anonymous'] = int(row['anonymous'])
        post_map['replies'] = []
        post_map['thread'] = int(row['thread'])
        post_map['parent'] = 0
        posts_data_list.append(post_map)
        inserted_set.add(post_id)

# remove the df
del content_df
del content_posters_df
del content_creation
del relations_df
del relations_creation_df

print('Total Posts inserted: ', len(inserted_set)) 
error_df = pd.read_csv('error_url.csv')  
error_urls = error_df['urls'].values.tolist()

for thread_id ,article, url in session.query(Articles.threads_id, Articles.raw_content, Articles.link).all():
    soup = BeautifulSoup(article, 'html.parser')
    try:                                      

        if url not in error_urls:                                      
            article_title = soup.select('#ArticleCol1 h1')
            
            article_raw_text = soup.select('#ArticleCol1 #Article')
                
            if len(article_raw_text) == 0:
                article_raw_text = soup.select('#ArticleCol1 .entry-content')
        else:
            article_title = soup.select('head > title')
            article_raw_text = soup.select('#Article')

        remove_block = article_raw_text[0].select('.SocialBlock')
        if remove_block:
            remove_block[0].decompose()
            
        article_map = defaultdict()
        article_map['thread_id'] = int(thread_id)
        article_map['header'] = str(article_title[0])
        article_map['body'] = str(article_raw_text[0])
        article_map['posts'] = [] # append the posts 
        article_data_list.append(article_map)
    except:
        print('Error for Article')
        print('Article thread: ', thread_id)

# https://stackoverflow.com/a/23499088/5916727
def depth(d, level=0):
    if len(d['replies'])==0:
        return level
    else:
        return max(depth(reply, level+1) for reply in d['replies'])

# get depth
posts_stack = posts_data_list[:]
while len(posts_stack)>0:
    post = posts_stack.pop()
    post['depth'] = depth(post)
    children = post['replies']
    for c in children:
        posts_stack.insert(0, c)
        
for post in posts_data_list:
    for article in article_data_list:
        if post['thread'] == article['thread_id']:
            article['posts'].append(post)

for article in article_data_list:
    article['posts'] = sorted(article['posts'], key=lambda k: k['creation_date']) 

with open('outjson_testing.json', 'w') as fp:
    json.dump(article_data_list, fp, indent=4)          
