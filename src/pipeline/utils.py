import psycopg2
from dotenv import load_dotenv
load_dotenv()
import os

def connect():
        return psycopg2.connect(
            user=os.getenv("user"),
            password=os.getenv("password"),
            host=os.getenv("host"),
            port=os.getenv("port"),
            database=os.getenv("database")
        )

def truncate_table(table_name, con, cur):
    with open("../sql/queries/truncate_table.sql") as f:
        sql = ' '.join(map(str, f.readlines()))% table_name
        print(sql)
        
        cur.execute(sql)       
        con.commit()

