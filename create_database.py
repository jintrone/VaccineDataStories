from sqlalchemy import *
import sys

def create_all_tables(drop=False):
    engine = create_engine('mysql://ats:ats@localhost/ATS2')
    metadata = MetaData()
    posters = Table('posters',metadata,
                    Column('id',Integer,Sequence('posters_seq'),primary_key=True),
                    Column('site_id',String(256)),
                    Column('username',String(1024)))


    threads = Table('threads',metadata,
                    Column('id',Integer,Sequence('thread_seq'),primary_key=True),
                    Column('site_id',String(256)),
                    Column('domain',String(2048)),
                    Column('title',String(2048)),
                    Column('creation_date',DateTime),
                    Column('last_scraped',DateTime),
                    Column('link',String(2048)),
                    Column('creator',None,ForeignKey('posters.id')))

    content = Table('content',metadata,
                    Column('id',Integer,Sequence('content_seq'),primary_key=True),
                    Column('site_id',String(256)),
                    Column('creator',None,ForeignKey('posters.id')),
                    Column('thread',None,ForeignKey('threads.id')),
                    Column('raw',Text),
                    Column('clean',Text),
                    Column('link',String(2048)),
                    Column('page',Integer),
                    Column('likes',Integer),
                    Column('creation_date',DateTime))

    replies = Table('relations',metadata,
                    Column('id',Integer,Sequence('replies_seq'),primary_key=True),
                    Column('src_id',None,ForeignKey('content.id')),
                    Column('targ_id',None,ForeignKey('content.id')),
                    Column('src_User',None,ForeignKey('posters.id')),
                    Column('targ_user',None,ForeignKey('posters.id')),
                    Column('type',String(128)))

    quote = Table('quotes',metadata,
                    Column('id',Integer,Sequence('quote_seq'),primary_key=True),
                    Column('src_id',None,ForeignKey('content.id')),
                    Column('targ_id',None,ForeignKey('content.id')),
                    Column('targuser',None,ForeignKey('posters.id')),
                    Column('content',Text))

    resources = Table('resources',metadata,
                      Column('id',Integer,Sequence('res_seq'),primary_key=True),
                      Column('link',String(2048)),
                      Column('src_id',None,ForeignKey('content.id')),
                      Column('type',String(128)))

    if drop:
        metadata.drop_all(engine)
    metadata.create_all(engine)



if __name__== "__main__":
    should_drop = False
    if len(sys.argv) > 0 and sys.argv[0]=="True":
        should_drop=True
    create_all_tables(drop=should_drop)












