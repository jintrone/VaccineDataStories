"""This program is for the connection of the DB which can get engine and session"""

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import json
import BaseModel

class DBConnect:
    
    def __init__(self):
        self.engine = None
        self.session = None

    def start_db(self, config_file):
        """"
        Summary
        ---------
        This function is to create dburl from the configuration file with kwargs
        and for initial setup of the engine and session
        
        Parameters
        -----------
        config_file: string
            json file (with path if in other directory) with the db configuration located in 'database_config'
            
        Returns
        -------
        None
            sets the global engine and session for the further use

        """
        with open(config_file) as f:
            database_configuration = json.load(f)['database_config']
    
        dburl_str = 'mysql+mysqlconnector://{user}:{password}@localhost/{db}'.format(**database_configuration)
        
        self.engine = create_engine(dburl_str)
        BaseModel.Base.metadata.create_all(self.engine)

        Session = sessionmaker(bind=self.engine)
        self.session = Session()
#        sqlalchemy.pool_recycle = 3600

        
    def get_engine(self):
        return self.engine

    def get_session(self):
        return self.session
    
    