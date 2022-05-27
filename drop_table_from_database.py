import sqlite3
import os
def delete_table_from_database(table_name,path_to_database):
    conn=sqlite3.connect(path_to_database)
    cur=conn.cursor()
    cur.execute('delete from {}'.format(table_name))
    conn.commit()
    conn.close()

def drop_table_from_database(table_name,path_to_database):
    conn=sqlite3.connect(path_to_database)
    cur=conn.cursor()
    cur.execute('drop table if exists {}'.format(table_name))
    conn.commit()
    conn.close()

table_name='stocks'
path_to_database=os.path.join(os.getcwd(),'datasets','sql_databases','tickers_companies_prices.db')
print(path_to_database)

drop_table_from_database(table_name,path_to_database)