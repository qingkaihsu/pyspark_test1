#!/usr/bin/env python
# coding: utf-8

# In[ ]:


from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import numpy as np
import pandas as pd
from pyspark.sql.types import StringType, IntegerType
from pyspark.sql.functions import udf
from pyspark.sql.functions import pandas_udf, PandasUDFType

spark = SparkSession.builder.appName("mldemo").getOrCreate()

for i in ['A_lvr_land_A','B_lvr_land_A','E_lvr_land_A','F_lvr_land_A','H_lvr_land_A']:
    df=pd.read_csv(i+'.csv')
    df.drop(df.head(1).index, inplace=True)
    df.to_csv(i+'_drop1.csv', index=False,encoding="utf-8_sig")
    
#讀檔
dfA=spark.read.csv("A_lvr_land_A_drop1.csv",sep = ',',header = True,inferSchema = True).withColumn('city', F.lit('台北市'))
dfB=spark.read.csv("B_lvr_land_A_drop1.csv",sep = ',',header = True,inferSchema = True).withColumn('city', F.lit('台中市'))
dfE=spark.read.csv("E_lvr_land_A_drop1.csv",sep = ',',header = True,inferSchema = True).withColumn('city', F.lit('高雄市'))
dfF=spark.read.csv("F_lvr_land_A_drop1.csv",sep = ',',header = True,inferSchema = True).withColumn('city', F.lit('新北市'))
dfH=spark.read.csv("H_lvr_land_A_drop1.csv",sep = ',',header = True,inferSchema = True).withColumn('city', F.lit('桃園市'))

#確認欄位相同，才能使用union()合併
dfA.columns == dfB.columns == dfE.columns == dfF.columns == dfH.columns

#合併檔案(確認欄位相同)
dfall=dfA.union(dfB).union(dfE).union(dfF).union(dfH)


#確認總數無誤
dfall.count()==dfA.count() + dfB.count() + dfE.count() + dfF.count() + dfH.count()

def chitonum(chi):
    if chi is None:return 0
    #刪掉"層"
    if chi[-1] == '層':chi = chi[:-1]
    num_cor = {'一':1, '二':2, '三':3, '四':4, '五':5, 
               '六':6, '七':7, '八':8, '九':9}
    num = 0
    #找 "十" 位置
    t_num = chi.find('十')
    # -1即為沒有十，取十位數前面的中文數字，若前面沒有中文數字即為十
    if t_num >= 0:
        num  += num_cor.get(chi[t_num-1:t_num],1) * 10
    if chi[-1] in num_cor: num  += num_cor[chi[-1]]
    return num

my_udf = udf(chitonum, IntegerType())
dfallf=dfall.withColumn('Floors',my_udf(F.col('總樓層數')))

#篩選
dffilter = dfallf.filter((F.col('主要用途') == '住家用') &
                         (F.col('建物型態').contains('住宅大樓')) &
                         (F.col('Floors')>=13))
dffilter.select('主要用途').distinct().show()
dffilter.select('建物型態').distinct().show()
dffilter.select('總樓層數').distinct().orderBy('總樓層數',ascending=0).show(100)

dfdate = dffilter.withColumn('date', F.to_date((F.col('交易年月日') + 19110000).cast(StringType()), 'yyyyMMdd')) 

df_preresult = dfdate.select('city' , 'date',
                          F.col('鄉鎮市區').alias('district'),
                          F.col('交易標的').alias('transaction_sign'),
                          F.col('土地區段位置建物區段門牌').alias('number_plate'),
                          F.col('土地移轉總面積平方公尺').alias('square_meter'),
                          F.col('都市土地使用分區').alias('use_zoning'),
                          F.col('非都市土地使用分區').alias('non_metropolis_district'),
                          F.col('非都市土地使用編定').alias('non_metropolis_land_use'),
                          F.col('交易年月日').alias('transaction_date'),
                          F.col('交易筆棟數').alias('transaction_number'),
                          F.col('移轉層次').alias('shifting'),
                          F.col('總樓層數').alias('total_floor'),
                          F.col('建物型態').alias('type'),
                          F.col('主要用途').alias('main_use'),
                          F.col('主要建材').alias('main_materials'),
                          F.col('建築完成年月').alias('complete_date'),
                          F.col('建物移轉總面積平方公尺').alias('total_area'),
                          F.col('建物現況格局-房').alias('room'),
                          F.col('建物現況格局-廳').alias('hall'),
                          F.col('建物現況格局-衛').alias('health'),
                          F.col('建物現況格局-隔間').alias('compartmented'),
                          F.col('有無管理組織').alias('organization'),
                          F.col('總價元').alias('price'),
                          F.col('單價元平方公尺').alias('unit_price'),
                          F.col('車位類別').alias('berth'),
                          F.col('車位移轉總面積平方公尺').alias('berth_area'),
                          F.col('車位總價元').alias('berth_price'),
                          F.col('備註').alias('note'),
                          F.col('編號').alias('number')
                         )


df_preresult_re=df_preresult.withColumn(
    "events",
    F.struct(
        F.col('district'),
        F.col('transaction_sign'),
        F.col('number_plate'),
        F.col('square_meter'),
        F.col('use_zoning'),
        F.col('non_metropolis_district'),
        F.col('non_metropolis_land_use'),
        F.col('transaction_date'),
        F.col('transaction_number'),
        F.col('shifting'),
        F.col('total_floor'),
        F.col('type'),
        F.col('main_use'),
        F.col('main_materials'),
        F.col('complete_date'),
        F.col('total_area'),
        F.col('room'),
        F.col('hall'),
        F.col('health'),
        F.col('compartmented'),
        F.col('organization'),
        F.col('price'),
        F.col('unit_price'),
        F.col('berth'),
        F.col('berth_area'),
        F.col('berth_price'),
        F.col('note'),
        F.col('number')
    )
).groupby(['city', 'date']).agg(F.collect_list('events').alias('events'))\
.orderBy('date', ascending=0)

df_result=df_preresult_re.withColumn(
  "time_slots",
  F.struct(
    F.col("date"),
    F.col("events")
  )).select('city','time_slots').groupby('city').agg(F.collect_list('time_slots').alias('time_slots'))
df_result.show()

df_result1,df_result2=df_result.randomSplit([0.5,0.5])
df_result1.show()
df_result2.show()

df_result1.repartition(1).write.mode('overwrite').json('output1.json')
df_result2.repartition(1).write.mode('overwrite').json('output2.json')


# In[ ]:




