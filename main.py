import pandas as pd
import psycopg2
import os
from dotenv import load_dotenv

def load_data(file,table):
    con = psycopg2.connect(
            host = os.getenv("HOST"),
            user = os.getenv("USERNAME"),
            password = os.getenv("PASSWORD"),
            port = os.getenv("PORT"),
            dbname = os.getenv("DBNAME")
    )
    cur = con.cursor()
    cur.execute(f"""truncate table {table}""")
    print(f"""{table} Successfully Truncated""")
    cur.execute(f"""
        COPY {table}
        FROM '{file}'
        DELIMITER ',' CSV;
    """)
    con.commit()
    cur.close()
    con.close()
    print(f"""{table} Successfully Inserted""")


if __name__ == '__main__':
    file = "/Users/jhunrielpiramide/data_engineering/csv_to_postgres/users.csv"
    table =  "users"
    load_dotenv()
    load_data(file,table)

    
    