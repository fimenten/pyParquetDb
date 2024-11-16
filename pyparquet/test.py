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


def concatUpdate(data_path: str, new_data: pd.DataFrame):
    data_path = pathlib.Path(DATA_DIR) / data_path
    # Read the data
    with filelock.FileLock(f"{data_path}"):
        data = pl.read_parquet(data_path)
        # Convert the new data to polars
        new_data = pl.DataFrame(new_data)
        # Concatenate the data
        data = pl.concat([data, new_data],how="vertical")
        # Write the data
        data.write_parquet(data_path)
        return data

def getPath(table:str, key:str,value:str):
    if not os.path.exists(DB_PATH):
        pd.DataFrame(columns=[key,"path"]).to_sql(table,sqlite3.connect(DB_PATH),index=False)
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(f"CREATE TABLE IF NOT EXISTS {table} ({key} TEXT PRIMARY KEY, path TEXT)")

    df = pd.read_sql(f"SELECT * FROM {table} WHERE {key}='{value}'", sqlite3.connect(DB_PATH)) 
    df = pl.from_pandas(df)
    if len(df) == 0:
        return None
    elif len(df) > 1:
        raise ValueError("Multiple rows found for the given key")
    return df.select("path").first().get("path")

def addOrGetPath(table:str, key:str, value:str):
    path = getPath(table,key,value)
    with filelock.FileLock(DB_PATH):
        if path is None:
            path = str(uuid.uuid4()) + ".parquet"
            data = pl.DataFrame({"table":[table],key:[value],"path":[path]})
            data = data.to_pandas().to_sql(table,sqlite3.connect(DB_PATH),if_exists="append",index=False)
        
    return path

def saveTo(table:str, key:str, value:str, data:pd.DataFrame):
    path = addOrGetPath(table,key,value)
    concatUpdate(path,data)
    return path


