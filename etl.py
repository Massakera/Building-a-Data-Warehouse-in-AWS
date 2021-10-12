from sql_queries import copy_table_queries,insert_table_queries
import psycopg2
import configparser

def load_staging_tables(conn,cur):
    for q in copy_table_queries:
        cur.execute(q)
        conn.commit()

def insert_tables(conn,cur):
    for q in insert_table_queries:
        cur.execute(q)
        conn.commit()

def main():
    config=configparser.ConfigParser()
    config.read('dwh.cfg')
    
    try:
        conn=psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
        cur=conn.cursor()
        load_staging_tables(conn,cur)
        insert_tables(conn,cur)
        conn.close()
    except psycopg2.Error as e:
        print(e)
        
if __name__=="__main__":
    main()