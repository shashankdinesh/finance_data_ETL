import pandas as pd
import os,datetime
#import pdb;pdb.set_trace()
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
os.chdir(os.getcwd())
file_path='TBT_20201014_1639.csv'
df_original = pd.read_csv(file_path)
Type,Time,Buy_Order,Sell_Order=df_original.columns[[5,6,7,8]].to_list()
print(Type,Time,Buy_Order,Sell_Order)
df_temp = df_original[[Type,Time,Buy_Order,Sell_Order]]
df_new_order = df_temp[df_temp[Type]=='N'].sort_values(by=[Time], ascending=[True])
min_value=float('-inf')
order_id=[]
for index, row in df_new_order.iterrows():
    if row[Buy_Order]>min_value:
        min_value=row[Buy_Order]
    else:order_id.append(row[Buy_Order])

df_final=df_original[(df_original [Buy_Order].isin(order_id)) | (df_original [Sell_Order].isin(order_id))]
#print(df_final[Time].values.tolist())
#df_final[Time]=pd.Series([datetime.datetime.fromtimestamp(date) for date in df_final[Time].values.tolist()])
df_final[Time]=pd.to_datetime(df_final[Time])
df_final.to_csv(os.path.join(BASE_DIR, 'report/result_{}.csv'.format(file_path.split('.')[0])),index=False)
