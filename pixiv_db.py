import sqlmodel
from .pixiv_sqlmodel import *


class PixivDB:
    def __init__(self, db_url: str):
        self.engine = sqlmodel.create_engine(db_url)
        sqlmodel.SQLModel.metadata.create_all(self.engine)
