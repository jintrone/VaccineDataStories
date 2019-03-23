"""All the models for the DB are defined here"""

from sqlalchemy import Column, Integer, String, ForeignKey, Sequence, DateTime, Text
from sqlalchemy.orm import relationship
import BaseModel
    
class Posters(BaseModel.Base):
    __tablename__ = 'posters'
    id = Column(Integer,Sequence('posters_seq'),primary_key=True)
    site_id = Column(String(256))
    username = Column(String(1024))
    name = Column(String(2048))
    anonymous = Column(Integer)
    threads = relationship('Threads', back_populates='posters')
    content = relationship('Content', back_populates='posters')
#    relations = relationship('Relations', back_populates='posters')

class Threads(BaseModel.Base):
    __tablename__ = 'threads'
    id = Column(Integer,Sequence('thread_seq'),primary_key=True)
    site_id = Column(String(256))
    domain = Column(String(2048))
    title = Column(String(2048))
    creation_date = Column(DateTime)
    last_scraped = Column(DateTime)
    link = Column(String(2048))
    creator = Column(None,ForeignKey('posters.id'))
    article = Column(Integer)
    posters = relationship('Posters', back_populates='threads')
    articles = relationship('Articles', back_populates='threads')
    content = relationship('Content', back_populates='threads')

class Articles(BaseModel.Base):
    __tablename__ = 'articles'
    id = Column(Integer,Sequence('article_seq'),primary_key=True)
    raw_content = Column(Text)
    clean_content = Column(Text)
    title = Column(String(2048))
    link = Column(String(2048))
    threads_id = Column(Integer, ForeignKey('threads.id'))
    threads = relationship('Threads', back_populates='articles')
    
class Content(BaseModel.Base):
    __tablename__ = 'content'
    
    id = Column(Integer,Sequence('content_seq'),primary_key=True)
    site_id = Column(String(256))
    creator = Column(None,ForeignKey('posters.id'))
    thread = Column(None,ForeignKey('threads.id'))
    raw = Column(Text)
    clean = Column(Text)
    link = Column(String(2048))
    page = Column(Integer)
    likes = Column(Integer)
    creation_date = Column(DateTime)
    posters = relationship('Posters', back_populates='content')
    threads = relationship('Threads', back_populates='content')
#    relations = relationship('Relations', back_populates='content')

    
class Relations(BaseModel.Base):
    
    __tablename__ = 'relations'
    
    id = Column(Integer,Sequence('replies_seq'),primary_key=True)
    src_id = Column(None,ForeignKey('content.id'))
    targ_id = Column(None,ForeignKey('content.id'))
    src_i = relationship('Content', foreign_keys=[src_id])

    target_i = relationship('Content', foreign_keys=[targ_id])

    src_user = Column(None,ForeignKey('posters.id'))

    targ_user = Column(None,ForeignKey('posters.id'))
    src = relationship('Posters', foreign_keys= [src_user])
    target = relationship('Posters', foreign_keys= [targ_user])

    type = Column(String(128))
    
#    content = relationship('Content', back_populates='relations')
#    posters = relationship('Posters', back_populates='relations')


    





