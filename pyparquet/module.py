import pandas as pd
import polars as pl
from dotenv import load_dotenv
import os
import pathlib
import filelock
import uuid
import sqlite3
load_dotenv()
DB_PATH = os.getenv("DB_PATH")
DATA_DIR = os.path.expanduser("~/data")

class parquetDb:
    def __init__(self,db_path:str=None,data_dir:str=None):
        if db_path is not None:
            self.db_path = db_path
        else:
            self.db_path = DB_PATH
        if data_dir is not None:
            self.data_dir = data_dir
        else:
            self.data_dir = DATA_DIR
        self.makeDb()
    def makeDb(self):
        if not os.path.exists(self.db_path):
            pd.DataFrame(columns=["table","key","path"]).to_sql("metadata",sqlite3.connect(self.db_path),index=False)
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("CREATE TABLE IF NOT EXISTS metadata (aaaa TEXT, key TEXT, path TEXT)")
        return self
    def checktable(self,table:str,key:str):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(f"CREATE TABLE IF NOT EXISTS {table} ({key} TEXT PRIMARY KEY, path TEXT)")
        return self

    def getAbsPath(self,table:str, key:str,value:str):
        self.checktable(table,key)
        df = pd.read_sql(f"SELECT * FROM {table} WHERE {key}='{value}'", sqlite3.connect(self.db_path)) 
        df = pl.from_pandas(df)
        if len(df) == 0:
            return None
        elif len(df) > 1:
            raise ValueError("Multiple rows found for the given key")
        return  df.select("path")[0,0]
    
    def createPath(self,table:str, key:str, value:str):
        path = pathlib.Path(self.data_dir) /(str(uuid.uuid4()) + ".parquet")
        path = str(path)
        data = pl.DataFrame({key:[value],"path":[path]})
        data = data.to_pandas().to_sql(table,sqlite3.connect(self.db_path),if_exists="append",index=False)
        return self
    
    def getAndCreatePath(self,table:str, key:str, value:str):
        absPath = self.getAbsPath(table,key,value)
        if absPath is None:
            self.createPath(table,key,value)
        return self.getAbsPath(table,key,value)
    
    def upsert(self,table:str, key:str, value:str, data:pd.DataFrame):
        path = self.getAndCreatePath(table,key,value)
        self.concatUpdate(path,data)
        return path

    
    def concatUpdate(self, data_path: str, new_data: pd.DataFrame):
        # Read the data
        print("data_path",data_path)
        if os.path.exists(data_path):
            try:
                data = pd.read_parquet(data_path)
            except Exception as e:
                # raise
                print(e)
                data = pd.DataFrame()
        else:
            data = pd.DataFrame()
        # Convert the new data to polars
        new_data = pd.DataFrame(new_data)
        # Concatenate the data
        data = pd.concat([data, new_data],axis=0)
        # Write the data
        print(data)
        data.to_parquet(data_path, index=False)
        return data

