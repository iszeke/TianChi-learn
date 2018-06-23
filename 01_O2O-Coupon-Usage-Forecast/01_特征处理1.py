import pandas as pd
import os
import feature_engineer
import warnings
warnings.filterwarnings("ignore")


# dataset split:
#            (date_received)
# dateset3: 20160701~20160731 (113640),features3 from 20160315~20160630  (off_test)
# dateset2: 20160515~20160615 (258446),features2 from 20160201~20160514
# dateset1: 20160414~20160514 (138303),features1 from 20160101~20160413



os.chdir(r'C:\Users\Zeke\Documents\Datasets\天池新人实战赛o2o优惠券使用预测')

off_train = pd.read_csv('ccf_offline_stage1_train.csv', keep_default_na=False)
off_test = pd.read_csv('ccf_offline_stage1_test_revised.csv', keep_default_na=False)
on_train = pd.read_csv('ccf_online_stage1_train.csv', keep_default_na=False)

# 划分数据集
dataset3 = off_test
feature3 = off_train[((off_train['Date']>='20160315')&(off_train['Date']>='20160630'))|((off_train['Date']=='null')&(off_train['Date_received']>='20160315')&(off_train['Date_received']<='20160630'))]

dataset2 = off_train[(off_train['Date_received']>='20160515')&(off_train['Date_received']<='20160615')]
feature2 = off_train[(off_train['Date']>='20160201')&(off_train['Date']<='20160514')|((off_train['Date']=='null')&(off_train['Date_received']>='20160201')&(off_train['Date_received']<='20160514'))]

dataset1 = off_train[(off_train['Date_received']>='20160414')&(off_train['Date_received']<='20160514')]
feature1 = off_train[(off_train['Date']>='20160101')&(off_train['Date']<='20160413')|((off_train['Date']=='null')&(off_train['Date_received']>='20160101')&(off_train['Date_received']<='20160413'))]

# 保存组合特征
os.chdir(r'C:\Users\Zeke\Desktop\新建文件夹')

# Merchant特征
merchant3_feature = feature_engineer.merchant_related(feature3)
merchant3_feature.to_csv('merchant3_feature.csv',index=None)

merchant2_feature = feature_engineer.merchant_related(feature3)
merchant2_feature.to_csv('merchant2_feature.csv',index=None)

merchant1_feature = feature_engineer.merchant_related(feature3)
merchant1_feature.to_csv('merchant1_feature.csv',index=None)

# User特征
user3_feature = feature_engineer.user_related(feature3)
user3_feature.to_csv('user3_feature.csv',index=None)

user2_feature = feature_engineer.user_related(feature2)
user2_feature.to_csv('user2_feature.csv',index=None)

user1_feature = feature_engineer.user_related(feature1)
user1_feature.to_csv('user1_feature.csv',index=None)
print('User特征完成')

# User_Merchant特征
user_merchant3 = feature_engineer.user_merchant(feature3)
user_merchant3.to_csv('user_merchant3.csv', index=None)

user_merchant2 = feature_engineer.user_merchant(feature2)
user_merchant2.to_csv('user_merchant2.csv', index=None)

user_merchant1 = feature_engineer.user_merchant(feature1)
user_merchant1.to_csv('user_merchant1.csv', index=None)
print('User_Merchant特征完成')

# Coupon特征
coupon3_feature = feature_engineer.coupon_related(dataset3,dtime=(2016,6,30))
coupon3_feature.to_csv('coupon3_feature.csv',index=None)

coupon2_feature = feature_engineer.coupon_related(dataset2,dtime=(2016,5,14))
coupon2_feature.to_csv('coupon2_feature.csv',index=None)

coupon1_feature = feature_engineer.coupon_related(dataset1,dtime=(2016,4,13))
coupon1_feature.to_csv('coupon1_feature.csv',index=None)
print('Coupon特征完成')

# Other特征
other3_feature = feature_engineer.other_feature(dataset3)
other3_feature.to_csv('other3_feature.csv', index=None)

other2_feature = feature_engineer.other_feature(dataset2)
other2_feature.to_csv('other2_feature.csv', index=None)

other1_feature = feature_engineer.other_feature(dataset1)
other1_feature.to_csv('other1_feature.csv', index=None)
print('Other特征完成')