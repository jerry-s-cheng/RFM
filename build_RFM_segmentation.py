# coding: utf-8
# ---------------------------------------------------------------------------------
# RFM data collection
# + Recency - amount of time since the customer’s most recent transaction [days_since_last_nonclub_order]
# + Frequency - total number of transactions made by the customer (during a defined period) [total_orders]
# + Monetary - total amount that the customer has spent across all transactions (during a defined period) [lifetime_value]
# ---------------------------------------------------------------------------------
# K-mean clustering
# Divide the customer list into tiered groups with more homogeneous characteristics for each of the three dimensions (R, F and M) via K-mean clustering method.
# Related link - http://scikit-learn.org/stable/modules/generated/sklearn.cluster.KMeans.html


# Load global libs
import pandas as pd
import os, time, sys
import psycopg2
from datetime import date, timedelta, datetime as DT
from sklearn.cluster import KMeans

# Time zone setup
os.environ['TZ'] = 'US/Pacific'
time.tzset()


# Utility functions and main
def _tstamp():
    """
    formatted time stamp
    """
    ts = time.time()
    # time.strftime('%X %x %Z')
    return '[{:s}]'.format(DT.fromtimestamp(ts).strftime('%m-%d %H:%M:%S'))

def get_order_data_from_db(qry_str):
    """
        extract data via sql query
        """
    conn = psycopg2.connect(dbname='matrix',
                            host='xxxxxxxx',
                            port='xxxxxxxx', user='xxxxxxxx', password='xxxxxxxx')
    df3 = pd.read_sql_query(qry_str, conn)
    conn.close()
    return df3

def main():
    qry_customer_RFM = '''
    SELECT
    	customer_id,
    	COALESCE(days_since_last_nonclub_order,0) AS recency,
    	total_orders AS frequency,
    	lifetime_value AS monetary
    FROM
    	customer_order
    WHERE lifetime_value >0 AND first_order_date >= '2009-04-13 21:06:20'
          AND first_order_date < current_date --limit 10000
    '''

    # Load customer RFM data into dataframe
    #df_cust_RFM = pd.read_csv('customer_RFM1.csv', delimiter=',')
    df_cust_RFM = get_order_data_from_db(qry_customer_RFM)


    # Normalize RFM value and assign tiers for each dimension
    l_attrs = ['recency','frequency','monetary']
    l_attrs_ln = [attr+'_ln' for attr in l_attrs]
    df_cust_RFM_norm = df_cust_RFM.copy()

    print('Normalize ln(x)...')
    for idx, attr in enumerate(l_attrs):
        attr_ln = l_attrs_ln[idx]
        df_cust_RFM_norm[attr_ln] = pd.np.log(df_cust_RFM_norm[attr]+1)
        val_min, val_max = df_cust_RFM_norm[attr_ln].min(), df_cust_RFM_norm[attr_ln].max()
        val_scal = val_max - val_min
        print(attr_ln, end=':\t')
        print(['{:.2f}'.format(val) for val in (val_min, val_max, val_scal)]) # Debug print
        df_cust_RFM_norm[attr] = df_cust_RFM_norm[attr_ln].apply(lambda x: (x - val_min) / val_scal)
        if attr[0] == 'r':
            df_cust_RFM[attr[0]+'_tier'] = df_cust_RFM_norm[attr].map(lambda nv: 'T{}'.format(int(nv*99.99//25)+1))
        else:
            df_cust_RFM[attr[0]+'_tier'] = df_cust_RFM_norm[attr].map(lambda nv: 'T{}'.format(4-int(nv*99.99//25)))

    # Perform K-mean clustering
    # Related link - http://scikit-learn.org/stable/modules/generated/sklearn.cluster.KMeans.html
    n_center = 64 # 4x4x4 RFM matrix
    est_km = KMeans(n_clusters= n_center, random_state=666, n_jobs= -3)
    print('# of center to cluster: {}'.format(n_center), flush=True)
    print(_tstamp()+' Start K-mean clustering...', flush=True) # Debug print
    tstart = time.time()
    est_km.fit(df_cust_RFM_norm[l_attrs])
    tfinish = time.time()
    print(_tstamp()+' Done! ({:.1f} sec)'.format(tfinish-tstart), flush=True) # Debug print

    # Assign cluster group
    df_cust_RFM['cgroup'] = est_km.labels_+1
    df_cust_RMF_centers = pd.DataFrame(est_km.cluster_centers_, columns=l_attrs)
    df_cust_RMF_centers.head()

    df_cust_RMF_centers.sort_values('monetary', ascending=False)
    df_cust_RFM.sort_values('customer_id', inplace=True)

    # Output to csv
    csv_file = 'customer_RFM_tiers.csv'
    df_cust_RFM.to_csv(csv_file, index=False)
    print (df_cust_RFM)
    print('Saved to csv: ' + csv_file)


if __name__ == '__main__':
    main()
