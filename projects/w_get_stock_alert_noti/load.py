from transform import transform
import psycopg2
import databaseconfig as cfg
import logging
logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s', datefmt='%d-%b-%y %H:%M:%S', level=logging.INFO)


def connect(params: dict):
    try:
        logging.info('Connecting to %s' % params["database"])
        conn = psycopg2.connect(host = params["host"],
                                database = params["database"],
                                user = params["user"],
                                password = params["password"])
        logging.info('Connected')
        return conn
    except Exception as error:
        logging.error(error)
        return None
    
conn = connect(cfg.postgresql)

list_data = transform()
def load_data():    
    sql = """
            INSERT INTO extnl.temp_table(ticker, isin_code, figi_code, company_name, notification_date, effective_date, reason, alert_code, alert_language, etldatetime) 
            VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
          """
    # 
    try:
        logging.info('uploading data...')
        cur = conn.cursor()
        cur.executemany(sql,list_data)
        conn.commit()
        logging.info('uploaded')
    except Exception as error:
        logging.error(error)
        
if __name__ == '__main__':
    load_data()
    