'''
Created on Aug 22, 2016

@author: eli
'''
'''
some style changes

'''
from datetime import date
from datetime import timedelta
import util


mon_tr={'JAN':1,'FEB':2, 'MAR':3,'APR':4,'MAY':5,'JUN':6, 'JUL':7,'AUG':8, 'SEP':9,'OCT':10, 'NOV':11,'DEC':12}
div_nums = ['199014', '139099']
pmt_status_codes = ['00','05','89']

'''
 gen_cc() by default query from yesterday to today, 
 get_cc(, 'dd-MMM-yyyy', n) will query on the specified date, starting from n days back
'''
 
def get_cc(db_con, error_code, end_date = None, step = 1):
    query_params = {}
    cc_list = []
    
    if end_date: 
        dd,mmm,yyyy = end_date.split('-')
        start_date = (date(int(yyyy), mon_tr[mmm], int(dd)) - timedelta(days = step)).strftime('%d-%b-%Y')
    else: 
        end_date = date.today().strftime('%d-%b-%Y')
        start_date= (date.today() - timedelta(days= step )).strftime('%d-%b-%Y')
        
    print 'start: {}, end: {}\n'.format(start_date, end_date)
    
    for div in div_nums:
        for sc in pmt_status_codes:
            query_params['division'] = div
            query_params['status_code'] = sc
            query_params['start_date'] = start_date
            query_params['end_date'] = end_date
            cc_list += util.get_cc_hash_from_db(db_con, query_params)
    
    for cc_hash, cnt_custid in cc_list:
        if cnt_custid > 1:
            print '{cc_hash}, # of cust_id: {cnt_custid}'.format(cc_hash = cc_hash, cnt_custid = cnt_custid)
            yield cc_hash, cnt_custid
            
            
def get_tx_by_cc(db_con, gen_cc):
    for cc_hash, cnt_custid in gen_cc:
        tx_recs = util.get_tx_detail_from_db(db_con, cc_hash)
        yield cc_hash, cnt_custid, tx_recs

def default_report():
    status_code = ['05','89','00']
    db_config = util.load_config()
    db_context = util.init_db_con(db_config)
 
    #gen_cc_hash = get_cc(db_context, status_code, '07-SEP-2016', 5) 
       
    gen_cc_hash = get_cc(db_context, status_code)
    gen_tx = get_tx_by_cc(db_context, gen_cc_hash)

    util.save_fraud_result(date.today(), gen_tx)    
    print "completed"
        
if __name__ == '__main__':
    default_report()
    