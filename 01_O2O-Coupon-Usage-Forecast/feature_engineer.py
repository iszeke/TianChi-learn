import pandas as pd
import numpy as np
from datetime import date
import warnings
warnings.filterwarnings("ignore")

def merchant_related(feature):
    '''
    1.merchant related:
          total_sales. sales_use_coupon. total_coupon
          merchant_min_distance,merchant_avg_distance,merchant_max_distance of those use coupon
          transfer_rate = sales_use_coupon/total_coupon.
          coupon_rate = sales_use_coupon/total_sales.
    '''
    merchant = feature[['Merchant_id','Coupon_id','Distance','Date_received','Date']]

    t = merchant[['Merchant_id']]
    t.drop_duplicates(inplace=True)

    t1 = merchant[merchant['Date']!='null'][['Merchant_id']]
    t1['totals_sales'] = 1
    t1 = t1.groupby('Merchant_id').agg('sum').reset_index()

    t2 = merchant[(merchant['Date']!='null')&(merchant['Coupon_id']!='null')][['Merchant_id']]
    t2['sales_use_coupon'] = 1
    t2 = t2.groupby('Merchant_id').agg('sum').reset_index()

    t3 = merchant[merchant['Coupon_id']!='null'][['Merchant_id']]
    t3['totals_coupon'] = 1
    t3 = t3.groupby('Merchant_id').agg('sum').reset_index()

    t4 = merchant[(merchant['Date']!='null')&(merchant['Coupon_id']!='null')][['Merchant_id','Distance']]
    t4.replace('null', -1, inplace=True)
    t4['Distance'] = t4['Distance'].astype('int')
    t4.replace(-1, np.nan, inplace=True)
    t5 = t4.groupby('Merchant_id').agg('min').reset_index()
    t5.rename(columns={'Distance':'merchant_min_distance'},inplace=True)
    t6 = t4.groupby('Merchant_id').agg('mean').reset_index()
    t6.rename(columns={'Distance': 'merchant_mean_distance'}, inplace=True)
    t7 = t4.groupby('Merchant_id').agg('max').reset_index()
    t7.rename(columns={'Distance': 'merchant_max_distance'}, inplace=True)
    t8 = t4.groupby('Merchant_id').agg('median').reset_index()
    t8.rename(columns={'Distance': 'merchant_median_distance'}, inplace=True)

    # 合并
    merchant_feature = pd.merge(t, t1, on='Merchant_id', how='left')
    merchant_feature = pd.merge(merchant_feature, t2, on='Merchant_id', how='left')
    merchant_feature = pd.merge(merchant_feature, t3, on='Merchant_id', how='left')
    merchant_feature = pd.merge(merchant_feature, t5, on='Merchant_id', how='left')
    merchant_feature = pd.merge(merchant_feature, t6, on='Merchant_id', how='left')
    merchant_feature = pd.merge(merchant_feature, t7, on='Merchant_id', how='left')
    merchant_feature = pd.merge(merchant_feature, t8, on='Merchant_id', how='left')

    merchant_feature['merchant_coupon_transfer_rate'] = merchant_feature['sales_use_coupon'].astype('float') / merchant_feature['totals_coupon']
    merchant_feature['coupon_rate'] = merchant_feature['sales_use_coupon'].astype('float') / merchant_feature['totals_sales']
    merchant_feature['sales_use_coupon'] = merchant_feature['sales_use_coupon'].replace(np.nan, 0)  # fillna with 0
    merchant_feature['totals_coupon'] = merchant_feature['totals_coupon'].replace(np.nan, 0)  # fillna with 0

    return merchant_feature

def coupon_related(dataset,dtime=(2016,6,30)):
    """
    2.coupon related:
          discount_rate. discount_man. discount_jian. is_man_jian
          day_of_week, day_of_month. (date_received)
          coupon_count
    """
    def get_discount_man(x):
        x = str(x).split(':')
        if len(x) == 1:
            return 'null'
        else:
            return int(x[0])
    def get_discount_jian(x):
        x = str(x).split(':')
        if len(x) == 1:
            return 'null'
        else:
            return int(x[1])
    def is_man_jian(x):
        x = str(x).split(':')
        if len(x) == 1:
            return 0
        else:
            return 1
    def calc_discount_rate(x):
        x = str(x).split(':')
        if len(x)== 1:
            return float(x[0])
        else:
            return 1.0-float(x[1])/float(x[0])

    coupon = dataset.copy()
    coupon['discount_man'] = coupon['Discount_rate'].apply(get_discount_man)
    coupon['discount_jian'] = coupon['Discount_rate'].apply(get_discount_jian)
    coupon['is_man_jian'] = coupon['Discount_rate'].apply(is_man_jian)
    coupon['discount_rate'] = coupon['Discount_rate'].apply(calc_discount_rate)
    coupon['day_of_week'] = coupon['Date_received'].astype('str').apply(
        lambda x: date(int(x[0:4]),int(x[4:6]),int(x[6:8])).weekday()+1)
    coupon['day_of_month'] = coupon['Date_received'].astype('str').apply(lambda x: int(x[4:6]))
    coupon['days_distance'] = coupon['Date_received'].astype('str').apply(
        lambda x: (date(int(x[0:4]),int(x[4:6]),int(x[6:8]))-date(dtime[0],dtime[1],dtime[2])).days)

    d = coupon[['Coupon_id']]
    d['coupon_count'] = 1
    d = d.groupby('Coupon_id').agg('sum').reset_index()
    coupon = pd.merge(coupon,d,on='Coupon_id',how='left')
    return coupon

def user_related(feature):
    """
    3.user related:
          count_merchant.
          user_avg_distance, user_min_distance,user_max_distance.
          buy_use_coupon. buy_total. coupon_received.
          buy_use_coupon/coupon_received.
          buy_use_coupon/buy_total
          user_date_datereceived_gap
    """
    user = feature.copy()
    t = user[['User_id']]
    t.drop_duplicates(inplace=True)

    t1 = user[user['Date']!='null'][['User_id', 'Merchant_id']]
    t1.drop_duplicates(inplace=True)
    t1['Merchant_id'] = 1
    t1 = t1.groupby('User_id').agg('sum').reset_index()
    t1.rename(columns={'Merchant_id':'count_merchant'},inplace=True)

    t2 = user[(user['Date']!='null')&(user['Coupon_id']!='null')][['User_id', 'Distance']]
    t2.replace('null',-1,inplace=True)
    t2['Distance'] = t2['Distance'].astype('int')
    t2.replace(-1,np.nan,inplace=True)
    t3 = t2.groupby('User_id').agg('min').reset_index()
    t3.rename(columns={'Distance':'user_min_distance'},inplace=True)
    t4 = t2.groupby('User_id').agg('mean').reset_index()
    t4.rename(columns={'Distance': 'user_mean_distance'}, inplace=True)
    t5 = t2.groupby('User_id').agg('max').reset_index()
    t5.rename(columns={'Distance': 'user_max_distance'}, inplace=True)
    t6 = t2.groupby('User_id').agg('median').reset_index()
    t6.rename(columns={'Distance': 'user_median_distance'}, inplace=True)

    t7 = user[(user['Date']!='null')&(user['Coupon_id']!='null')][['User_id']]
    t7['buy_use_coupon'] = 1
    t7 = t7.groupby('User_id').agg('sum').reset_index()

    t8 = user[(user['Date']!='null')][['User_id']]
    t8['buy_total'] = 1
    t8 = t8.groupby('User_id').agg('sum').reset_index()

    t9 = user[(user['Coupon_id']!='null')][['User_id']]
    t9['coupon_received'] = 1
    t9 = t9.groupby('User_id').agg('sum').reset_index()

    def get_date_datereceived_gap(x):
        x = str(x).split(':')
        d1 = date(int(x[0][0:4]),int(x[0][4:6]),int(x[0][6:8]))
        d2 = date(int(x[1][0:4]),int(x[1][4:6]),int(x[1][6:8]))
        return (d1-d2).days


    t10 = user[(user['Date_received']!='null')&(user['Date']!='null')][['User_id','Date_received','Date']]
    t10['user_date_datereceived_gap'] = t10['Date'] + ':' + t10['Date_received']
    t10['user_date_datereceived_gap'] = t10['user_date_datereceived_gap'].apply(get_date_datereceived_gap)
    t10 = t10[['User_id','user_date_datereceived_gap']]
    t11 = t10.groupby('User_id').agg('min').reset_index()
    t11.rename(columns={'user_date_datereceived_gap':'min_user_date_datereceived_gap'},inplace=True)
    t12 = t10.groupby('User_id').agg('mean').reset_index()
    t12.rename(columns={'user_date_datereceived_gap':'mean_user_date_datereceived_gap'},inplace=True)
    t13 = t10.groupby('User_id').agg('max').reset_index()
    t13.rename(columns={'user_date_datereceived_gap':'max_user_date_datereceived_gap'},inplace=True)

    user_feature = pd.merge(t, t1, on='User_id', how='left')
    user_feature = pd.merge(user_feature, t3, on='User_id', how='left')
    user_feature = pd.merge(user_feature, t4, on='User_id', how='left')
    user_feature = pd.merge(user_feature, t5, on='User_id', how='left')
    user_feature = pd.merge(user_feature, t6, on='User_id', how='left')
    user_feature = pd.merge(user_feature, t7, on='User_id', how='left')
    user_feature = pd.merge(user_feature, t8, on='User_id', how='left')
    user_feature = pd.merge(user_feature, t9, on='User_id', how='left')
    user_feature = pd.merge(user_feature, t11, on='User_id', how='left')
    user_feature = pd.merge(user_feature, t12, on='User_id', how='left')
    user_feature = pd.merge(user_feature, t13, on='User_id', how='left')

    user_feature['count_merchant'] = user_feature['count_merchant'].replace(np.nan, 0)
    user_feature['buy_use_coupon'] = user_feature['buy_use_coupon'].replace(np.nan, 0)
    user_feature['buy_use_coupon_rate'] = user_feature['buy_use_coupon'].astype('float') / user_feature['buy_total'].astype('float')
    user_feature['user_coupon_transfer_rate'] = user_feature['buy_use_coupon'].astype('float') / user_feature['coupon_received'].astype('float')

    user_feature['buy_total'] = user_feature['buy_total'].replace(np.nan, 0)
    user_feature['coupon_received'] = user_feature['coupon_received'].replace(np.nan, 0)

    return user_feature

def other_feature(dataset):
    """
    5. other feature:
          this_month_user_receive_all_coupon_count
          this_month_user_receive_same_coupon_count
          this_month_user_receive_same_coupon_lastone
          this_month_user_receive_same_coupon_firstone
          this_day_user_receive_all_coupon_count
          this_day_user_receive_same_coupon_count
          day_gap_before, day_gap_after  (receive the same coupon)
    """
    other = dataset.copy()
    t = other[['User_id']]
    t['this_month_user_receive_all_coupon_count'] = 1
    t = t.groupby('User_id').agg('sum').reset_index()

    t1 = other[['User_id', 'Coupon_id']]
    t1['this_month_user_receive_same_coupon_count'] = 1
    t1 = t1.groupby(['User_id', 'Coupon_id']).agg('sum').reset_index()

    t2 = other[['User_id', 'Coupon_id', 'Date_received']]
    t2['Date_received'] = t2['Date_received'].astype('str')
    t2 = t2.groupby(['User_id', 'Coupon_id'])['Date_received'].agg(lambda x: ':'.join(x)).reset_index()
    t2['receive_number'] = t2['Date_received'].apply(lambda x: len(x.split(':')))
    t2 = t2[t2['receive_number']>1]
    t2['max_date_received'] = t2['Date_received'].apply(lambda x: max([int(d) for d in x.split(':')]))
    t2['min_date_received'] = t2['Date_received'].apply(lambda x: min([int(d) for d in x.split(':')]))
    t2 = t2[['User_id', 'Coupon_id', 'max_date_received', 'min_date_received']]

    t3 = other[['User_id', 'Coupon_id', 'Date_received']]
    t3 = pd.merge(t3, t2, on=['User_id', 'Coupon_id'], how='left')
    t3['this_month_user_receive_same_couple_lastone'] = t3['max_date_received'] - t3['Date_received'].astype('int')
    t3['this_month_user_receive_same_couple_firstone'] = t3['Date_received'].astype('int') - t3['min_date_received']

    def is_firstlastone(x):
        if x == 0:
            return 1
        elif x > 0:
            return 0
        else:
            return -1
    t3['this_month_user_receive_same_couple_lastone'] = t3['this_month_user_receive_same_couple_lastone'].apply(is_firstlastone)
    t3['this_month_user_receive_same_couple_firstone'] = t3['this_month_user_receive_same_couple_firstone'].apply(is_firstlastone)

    t4 = other[['User_id', 'Date_received']]
    t4['this_day_user_receive_all_coupon_count'] = 1
    t4 = t4.groupby(['User_id', 'Date_received']).agg('sum').reset_index()

    t5 = other[['User_id', 'Coupon_id', 'Date_received']]
    t5['this_day_user_receive_same_couple_count'] = 1
    t5 = t5.groupby(['User_id', 'Coupon_id', 'Date_received']).agg('sum').reset_index()

    t6 = other[['User_id', 'Coupon_id', 'Date_received']]
    t6['Date_received'] = t6['Date_received'].astype('str')
    t6 = t6.groupby(['User_id', 'Coupon_id'])['Date_received'].agg(lambda x: ':'.join(x)).reset_index()
    t6.rename(columns={'Date_received':'dates'}, inplace=True)

    def get_day_gap_before(x):
        date_received, dates = x.split('-')
        dates = dates.split(':')
        gaps = []
        for d in dates:
            c1 = date(int(date_received[0:4]),int(date_received[4:6]),int(date_received[6:8]))
            c2 = date(int(d[0:4]),int(d[4:6]),int(d[6:8]))
            this_gap = (c1 - c2).days
            if this_gap > 0:
                gaps.append(this_gap)
        if len(gaps) == 0:
            return -1
        else:
            return min(gaps)
    def get_day_gap_after(x):
        date_received, dates = x.split('-')
        dates = dates.split(':')
        gaps = []
        for d in dates:
            c1 = date(int(date_received[0:4]),int(date_received[4:6]),int(date_received[6:8]))
            c2 = date(int(d[0:4]),int(d[4:6]),int(d[6:8]))
            this_gap = (c1 - c2).days
            if this_gap < 0:
                gaps.append(this_gap)
        if len(gaps) == 0:
            return -1
        else:
            return min(gaps)

    t7 = other[['User_id', 'Coupon_id', 'Date_received']]
    t7 = pd.merge(t7, t6, on=['User_id', 'Coupon_id'], how='left')
    t7['date_received_and_dates'] = t7['Date_received'].astype('str') + '-' + t7['dates']
    t7['day_gap_before'] = t7['date_received_and_dates'].apply(get_day_gap_before)
    t7['day_gap_after'] = t7['date_received_and_dates'].apply(get_day_gap_after)
    t7 = t7[['User_id', 'Coupon_id', 'Date_received', 'day_gap_before', 'day_gap_after']]

    # 合并
    other_feature = pd.merge(t, t1, on='User_id')
    other_feature = pd.merge(other_feature, t3, on=['User_id', 'Coupon_id'])
    other_feature = pd.merge(other_feature, t4, on=['User_id', 'Date_received'])
    other_feature = pd.merge(other_feature, t5, on=['User_id', 'Coupon_id', 'Date_received'])
    other_feature = pd.merge(other_feature, t7, on=['User_id', 'Coupon_id', 'Date_received'])

    return other_feature

def user_merchant(feature):
    """
    4.user_merchant:
          times_user_buy_merchant_before.
    """
    um = feature.copy()
    t = um[['User_id', 'Merchant_id']]
    t.drop_duplicates(inplace=True)

    t1 = um[['User_id', 'Merchant_id', 'Date']]
    t1 = t1[t1['Date']!='null'][['User_id', 'Merchant_id']]
    t1['user_merchant_buy_total'] = 1
    t1 = t1.groupby(['User_id', 'Merchant_id']).agg('sum').reset_index()

    t2 = um[['User_id', 'Merchant_id', 'Coupon_id']]
    t2 = t2[t2['Coupon_id']!='null'][['User_id', 'Merchant_id']]
    t2['user_merchant_received'] = 1
    t2 = t2.groupby(['User_id', 'Merchant_id']).agg('sum').reset_index()
    t2.drop_duplicates(inplace=True)

    t3 = um[['User_id', 'Merchant_id', 'Date', 'Date_received']]
    t3 = t3[(t3['Date']!='null')&(t3['Date_received']!='null')][['User_id', 'Merchant_id']]
    t3['user_merchant_buy_use_coupon'] = 1
    t3 = t3.groupby(['User_id', 'Merchant_id']).agg('sum').reset_index()
    t3.drop_duplicates(inplace=True)

    t4 = um[['User_id', 'Merchant_id']]
    t4['user_merchant_any'] = 1
    t4 = t4.groupby(['User_id', 'Merchant_id']).agg('sum').reset_index()
    t4.drop_duplicates(inplace=True)

    t5 = um[['User_id', 'Merchant_id', 'Date', 'Coupon_id']]
    t5 = t5[(t5['Date']!='null')&(t5['Coupon_id']=='null')][['User_id', 'Merchant_id']]
    t5['user_merchant_buy_common'] = 1
    t5 = t5.groupby(['User_id', 'Merchant_id']).agg('sum').reset_index()
    t5.drop_duplicates(inplace=True)

    #合并
    umf = pd.merge(t, t1, on=['User_id', 'Merchant_id'], how='left')
    umf = pd.merge(umf, t2, on=['User_id', 'Merchant_id'], how='left')
    umf = pd.merge(umf, t3, on=['User_id', 'Merchant_id'], how='left')
    umf = pd.merge(umf, t4, on=['User_id', 'Merchant_id'], how='left')
    umf = pd.merge(umf, t5, on=['User_id', 'Merchant_id'], how='left')
    umf['user_merchant_buy_use_coupon'] = umf['user_merchant_buy_use_coupon'].replace(np.nan,0)
    umf['user_merchant_buy_common'] = umf['user_merchant_buy_common'].replace(np.nan,0)
    umf['user_merchant_coupon_transfer_rate'] = umf['user_merchant_buy_use_coupon'].astype('float') / umf['user_merchant_received'].astype('float')
    umf['user_merchant_coupon_buy_rate'] = umf['user_merchant_buy_use_coupon'].astype('float') / umf['user_merchant_buy_total'].astype('float')
    umf['user_merchant_rate'] = umf['user_merchant_buy_total'].astype('float') / umf['user_merchant_any'].astype('float')
    umf['user_merchant_common_buy_rate'] = umf['user_merchant_buy_use_coupon'].astype('float') / umf['user_merchant_buy_total'].astype('float')

    return umf
