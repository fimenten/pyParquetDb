from pyparquet import parquetDb
import pandas as pd
import sqlite3

inst = parquetDb()

def senario_insert():
    inst = parquetDb()
    df = pd.DataFrame({"a":[1,2,3],"b":[4,5,6]})
    inst.upsert("test","a","1",df.copy())
    inst.upsert("test","a","2",df.copy())
    
    dfpath1 = inst.getAndCreatePath("test","a","1")
    dfpath2 = inst.getAndCreatePath("test","a","2")
    print(dfpath1)
    print(dfpath2)
    
    df1 = pd.read_parquet(dfpath1)
    df2 = pd.read_parquet(dfpath2)
    print(len(df1.index))
    print(len(df2.index))
    print()
    with sqlite3.Connection(inst.db_path) as conn:
       meta = pd.read_sql("SELECT * FROM test",conn)
       print(meta)
    
def senario_fetch():
    senario_insert()
    inst = parquetDb()
    dfpath1 = inst.getAndCreatePath("test","a","1")
    dfpath2 = inst.getAndCreatePath("test","a","2")
    print(dfpath1)
    print(dfpath2)
    df1 = pd.read_parquet(dfpath1)
    df2 = pd.read_parquet(dfpath2)
    print(df1)
    print(df2)


senario_insert()