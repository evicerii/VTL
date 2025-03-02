'''
base settings for connecting to db
'''
from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    '''Base settings'''
    DB_HOST:str
    DB_PORT:int
    DB_USER:str
    DB_PASS:str
    DB_NAME:str

    @property
    def database_url_mysql(self):
        '''
        return connect url
        '''
        return f'mysql://{self.DB_USER}:{self.DB_PASS}@{
            self.DB_HOST}:{self.DB_PORT}/{
                self.DB_NAME}?charset=utf8'
    model_config = SettingsConfigDict(env_file='.env')
settings = Settings()
