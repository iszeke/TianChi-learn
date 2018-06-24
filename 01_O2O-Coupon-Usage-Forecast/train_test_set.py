import pandas as pd
import numpy as np
from datetime import date
import os
"""
合并分解的特征
generate training and testing set
"""


def get_label(s):
    s = s.split(':')
    if s[0]=='null':
        return 0
    elif (date(int(s[0][0:4]),int(s[0][4:6]),int(s[0][6:8]))-date(int(s[1][0:4]),int(s[1][4:6]),int(s[1][6:8]))).days<=15:
        return 1
    else:
        return -1


if __name__ == '__main__':

    os.chdir(r'C:\Users\Zeke\Desktop\新建文件夹')

    coupon3 = pd.read_csv('coupon3_feature.csv', keep_default_na=False)
    merchant3 = pd.read_csv('merchant3_feature.csv', keep_default_na=False)
    user3 = pd.read_csv('user3_feature.csv', keep_default_na=False)
    user_merchant3 = pd.read_csv('user_merchant3.csv', keep_default_na=False)
    other_feature3 = pd.read_csv('other3_feature.csv', keep_default_na=False)

    dataset3 = pd.merge(coupon3,merchant3,on='Merchant_id',how='left')
    dataset3 = pd.merge(dataset3,user3,on='User_id',how='left')
    dataset3 = pd.merge(dataset3,user_merchant3,on=['User_id','Merchant_id'],how='left')
    dataset3 = pd.merge(dataset3,other_feature3,on=['User_id','Coupon_id','Date_received'],how='left')
    dataset3.drop_duplicates(inplace=True)
    print(dataset3.shape)

    dataset3['user_merchant_buy_total'] = dataset3['user_merchant_buy_total'].replace(np.nan,0)
    dataset3['user_merchant_any'] = dataset3['user_merchant_any'].replace(np.nan,0)
    dataset3['user_merchant_received'] = dataset3['user_merchant_received'].replace(np.nan,0)
    dataset3['is_weekend'] = dataset3['day_of_week'].apply(lambda x:1 if x in (6,7) else 0)
    weekday_dummies = pd.get_dummies(dataset3['day_of_week'])
    weekday_dummies.columns = ['weekday'+str(i+1) for i in range(weekday_dummies.shape[1])]
    dataset3 = pd.concat([dataset3,weekday_dummies],axis=1)

    dataset3.drop(['Merchant_id','day_of_week','coupon_count'],axis=1,inplace=True)
    dataset3 = dataset3.replace('null',np.nan)
    dataset3.to_csv('dataset3.csv',index=None)

    #########
    coupon2 = pd.read_csv('coupon2_feature.csv', keep_default_na=False)
    merchant2 = pd.read_csv('merchant2_feature.csv', keep_default_na=False)
    user2 = pd.read_csv('user2_feature.csv', keep_default_na=False)
    user_merchant2 = pd.read_csv('user_merchant2.csv', keep_default_na=False)
    other_feature2 = pd.read_csv('other2_feature.csv', keep_default_na=False)

    dataset2 = pd.merge(coupon2, merchant2, on='Merchant_id', how='left')
    dataset2 = pd.merge(dataset2, user2, on='User_id', how='left')
    dataset2 = pd.merge(dataset2, user_merchant2, on=['User_id', 'Merchant_id'], how='left')
    dataset2 = pd.merge(dataset2, other_feature2, on=['User_id', 'Coupon_id', 'Date_received'], how='left')
    dataset2.drop_duplicates(inplace=True)
    print(dataset2.shape)

    dataset2['user_merchant_buy_total'] = dataset2['user_merchant_buy_total'].replace(np.nan,0)
    dataset2['user_merchant_any'] = dataset2['user_merchant_any'].replace(np.nan,0)
    dataset2['user_merchant_received'] = dataset2['user_merchant_received'].replace(np.nan,0)
    dataset2['is_weekend'] = dataset2['day_of_week'].apply(lambda x:1 if x in (6,7) else 0)
    weekday_dummies = pd.get_dummies(dataset2['day_of_week'])
    weekday_dummies.columns = ['weekday'+str(i+1) for i in range(weekday_dummies.shape[1])]
    dataset2 = pd.concat([dataset2,weekday_dummies],axis=1)

    dataset2['label'] = dataset2['Date'].astype('str') + ':' +  dataset2['Date_received'].astype('str')
    print(dataset2['label'])
    dataset2.label = dataset2.label.apply(get_label)
    dataset2.drop(['Merchant_id','day_of_week','Date','Date_received','Coupon_id','coupon_count'],axis=1,inplace=True)
    dataset2 = dataset2.replace('null',np.nan)
    dataset2.to_csv('dataset2.csv',index=None)
    #
    ####
    coupon1 = pd.read_csv('coupon1_feature.csv', keep_default_na=False)
    merchant1 = pd.read_csv('merchant1_feature.csv', keep_default_na=False)
    user1 = pd.read_csv('user1_feature.csv', keep_default_na=False)
    user_merchant1 = pd.read_csv('user_merchant1.csv', keep_default_na=False)
    other_feature1 = pd.read_csv('other1_feature.csv', keep_default_na=False)

    dataset1 = pd.merge(coupon1, merchant1, on='Merchant_id', how='left')
    dataset1 = pd.merge(dataset1, user1, on='User_id', how='left')
    dataset1 = pd.merge(dataset1, user_merchant1, on=['User_id', 'Merchant_id'], how='left')
    dataset1 = pd.merge(dataset1, other_feature1, on=['User_id', 'Coupon_id', 'Date_received'], how='left')
    dataset1.drop_duplicates(inplace=True)
    print(dataset1.shape)

    dataset1['user_merchant_buy_total'] = dataset1['user_merchant_buy_total'].replace(np.nan, 0)
    dataset1['user_merchant_any'] = dataset1['user_merchant_any'].replace(np.nan, 0)
    dataset1['user_merchant_received'] = dataset1['user_merchant_received'].replace(np.nan, 0)
    dataset1['is_weekend'] = dataset1['day_of_week'].apply(lambda x: 1 if x in (6, 7) else 0)
    weekday_dummies = pd.get_dummies(dataset1['day_of_week'])
    weekday_dummies.columns = ['weekday' + str(i + 1) for i in range(weekday_dummies.shape[1])]
    dataset1 = pd.concat([dataset1, weekday_dummies], axis=1)

    dataset1['label'] = dataset1['Date'].astype('str') + ':' + dataset1['Date_received'].astype('str')
    print(dataset1['label'])
    dataset1.label = dataset1.label.apply(get_label)
    dataset1.drop(['Merchant_id', 'day_of_week', 'Date', 'Date_received', 'Coupon_id', 'coupon_count'], axis=1,
                  inplace=True)
    dataset1 = dataset1.replace('null', np.nan)
    dataset1.to_csv('dataset1.csv', index=None)






































