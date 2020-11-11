#python dropbox_to_mysql.py dropbox_to_mysql.config
import os,sys
import dropbox
import pandas as pd
import mysql.connector
import configparser
from sqlalchemy import create_engine
import logging
logging.basicConfig(filename='dropbox_to_mysql.log', level=logging.DEBUG)
config = configparser.ConfigParser()
config_file = sys.argv[1]
config.read(config_file)
logging.info("Reading configurations from : {}".format(config_file))
dropbox_api_key = config['DROPBOX_CONFIG']['api_key']
dropbox_file_path = config['DROPBOX_CONFIG']['file_path']
dropbox_local_file_path = config['DROPBOX_CONFIG']['local_file_path']
mysql_host=config['MYSQL_CONFIG']['host']
mysql_user=config['MYSQL_CONFIG']['user']
mysql_password=config['MYSQL_CONFIG']['password']
mysql_database=config['MYSQL_CONFIG']['database']
mysql_port=config['MYSQL_CONFIG']['port']
mysql_table=config['MYSQL_CONFIG']['table']
mysql_final_merge=config['MERGE_SQL']['mysql_merge_sql']
try:
    dbx = dropbox.Dropbox(dropbox_api_key)
    logging.info("Downloading data from dropbox location : {} to local location : {}".format(dropbox_file_path, dropbox_local_file_path))
    dbx.files_download_to_file(dropbox_local_file_path,dropbox_file_path,rev=None)
    logging.info("Download completed")
except Exception as e:
    logging.error("Error while downlaoding data from dropbox with error {}".format(e))
    exit()
df = pd.read_csv(dropbox_local_file_path)
logging.info("Creating engine for MySQL database")
engine = create_engine('mysql+mysqlconnector://'+mysql_user+':'+mysql_password+'@'+mysql_host+':'+mysql_port+'/'+mysql_database, echo=False)   
logging.info("Loading data into {} table".format(mysql_table))
try:
    df.to_sql(name=mysql_table, con=engine, if_exists='replace', index=False)
    logging.info("Loading completed, merge is about to start.")
    with engine.connect() as conn:
        conn.execute(mysql_final_merge)
except Exception as e:
    logging.error("Error while loading/merging data into table with exccpetion {}".format(e))
    exit()
logging.info("Data load and merge to final table completed. Removing files now.")
os.remove(dropbox_local_file_path)
logging.info("Script completed.")
