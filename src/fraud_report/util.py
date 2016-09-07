'''
Created on Aug 18, 2016

@author: eli
'''
import cx_Oracle
import os
from datetime import datetime
import ConfigParser

class db(object):
    cc_hash = []
    tx = []
    def __init__(self, cfg=None):
        if not cfg:
            self.db_uname = ''
            self.db_passwd =''
            self.db_queries = []
        else:
            self.db_uname = cfg.uname 
            self.db_passwd = cfg.passwd
            self.db_queries = cfg.queries
    def get_cc_hash_from_db(self):
        return self.cc_hash
    def get_tx_details_from_db(self, cc_hash):
        return self.tx
    
        

def load_config():
    SQL_LIST = []
    src_dir = os.path.dirname(os.path.abspath(__file__))
    cfg_file = os.path.join(os.path.split(src_dir)[0],'config.ini')
 
    cfg = ConfigParser.ConfigParser()
    cfg.read(cfg_file)
    UNAME = cfg.get('Database','uname')
    PASS = cfg.get('Database','passwd')
    SQL_LIST.append(cfg.get('Database','SQL_1').replace('\n',' ').replace('\t',' '))
    SQL_LIST.append(cfg.get('Database','SQL_2'))
    return UNAME, PASS, SQL_LIST
    
def init_db_con(db_config):
    uname, passwd, SQL_list = db_config
    db_con = cx_Oracle.connect(uname, passwd, 'CT')
    my_cur = db_con.cursor()
    return my_cur, SQL_list


def get_cc_hash_from_db(db_context, query_params ):
    
    my_cur, SQL_list = db_context
    SQL  = SQL_list[0]
    
    if len(query_params) <> 4: 
        print 'Error, need 4 query parameters'
    else:
        for para_key in ['division', 'start_date','end_date','status_code']:
            if not query_params.has_key(para_key):
                print '{0:} is missing\n'.format(para_key)

    my_cur.execute(SQL, query_params)
    recs =  my_cur.fetchall()
    return recs

''' get the POs based on cc hash, then get tx details of those POs in recent 30 days '''

def get_tx_detail_from_db(db_context, cc_hash):
    my_cur, SQL_list = db_context
    SQL = SQL_list[1]
    my_cur.execute(SQL, {'cc_hash':cc_hash})
    recs = my_cur.fetchall()
    return recs

def save_fraud_result(my_date, gen_result):
    home_dir = os.path.expanduser('~')
    try:
        fname = my_date.strftime('%Y%m%d') + '.txt'
    except TypeError:
        print 'save_fraud_result requires datetime type'
        return 
    
    dst_dir = os.path.join(home_dir, 'My Documents', 'Payment', 'Fraud Reports')
    if not os.path.exists(dst_dir):
        os.mkdir(dst_dir)
    dst = os.path.join(dst_dir, fname)
    if os.path.exists(dst):
        fdir = os.path.dirname(dst)
        suffix = '_' + datetime.now().strftime('%f')[:3] + '.txt'
        fname = os.path.basename(dst).rsplit('.',1)[0] + suffix
        dst = os.path.join(fdir,fname)  
    
    with open(dst,'wt') as fh:
        for cc_hash, custid_cnt, tx_recs in gen_result:
            fh.write('CC Hash = {0:}  shared by {1:} customer_ids\n'.format(cc_hash,custid_cnt)) 
            header = '{CUST_ID:<10s} {EMAIL:<25s} {PO:<10s} {IP:<15s} {PO_DATE:<10s}\n'.format(CUST_ID = 'CUST_ID', EMAIL = 'EMAIL', PO ='PO', IP = 'IP', PO_DATE = 'PO_DATE')
            fh.write(header)                
            for cust_id, email,PO, IP, PO_date in tx_recs:
                cl = '{CUST_ID:<10d} {EMAIL:<25s} {PO:<10d} {IP:<15s} {PO_DATE:10s}\n'.format(CUST_ID = cust_id, EMAIL = email, PO = PO, IP = IP, PO_DATE = PO_date.strftime('%Y-%m-%d'))
                fh.write(cl)

        