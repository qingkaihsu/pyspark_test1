#!/usr/bin/env python
# coding: utf-8

# In[1]:


'''
撰寫網路爬蟲程式，抓取 【 內政部不動產時價登錄網 】 中
位於 
【 臺北市 新北市 桃園市 臺中市 高雄市 】
【 不動產買賣 】 資料。
請選擇 
【 非本期下載 】
【 CSV 格式 】
【 資料內容 發布日期108 年第 2 季 】。

以下為第一題
資料蒐集(爬蟲)
選取條件
檔案下載
'''

#操作 browser 的 API
from selenium import webdriver
#等待某個元素出現的工具
from selenium.webdriver.support.ui import WebDriverWait
#和 WebDriverWait 使用，若條件發生，則等待結束，往下一行執行
from selenium.webdriver.support import expected_conditions as EC
#和 EC、WebDriverWait 使用，期待元素出現要透過什麼方式指定
from selenium.webdriver.common.by import By
#選取select 選單
from selenium.webdriver.support.ui import Select
#等待
from time import sleep
#抓取當下檔案路徑。判斷檔案是否存在。
import os
#解壓縮
import zipfile
#存取csv
import pandas as pd

#瀏覽器設定，不開啟實體瀏覽器(檔案存於當下工作目錄)。
options = webdriver.ChromeOptions()
options.add_argument("--headless")      #不開啟實體瀏覽器背景執行
driver = webdriver.Chrome( options = options )

def crawler():
    #走訪頁面
    driver.get('https://plvr.land.moi.gov.tw/DownloadOpenData#tab_opendata_history_content')
    sleep(3)
    
    #按下非本期，等待該選項出現
    WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.CSS_SELECTOR, "#ui-id-2")))
    driver.find_element(By.CSS_SELECTOR,"#ui-id-2").click() 
    sleep(5)
    
    #按下csv
    select = Select(driver.find_element(By.CSS_SELECTOR,"#fileFormatId"))
    select.select_by_value("csv")
    print("click csv")
    sleep(1)
    
    #按下10802
    select = Select(driver.find_element(By.CSS_SELECTOR,"#historySeason_id"))
    select.select_by_value("108S2")
    print("click 10802")
    sleep(1)
    
    #按下進階搜尋
    driver.find_element(By.CSS_SELECTOR,"#downloadTypeId2").click() 
    print("click further research")
    sleep(3)
    
    #按下台北市、新北市、桃園市、台中市、高雄市不動產買賣
    driver.find_element_by_xpath('//*[@id="table5"]/tbody/tr[7]/td[2]/input').click()
    driver.find_element_by_xpath('//*[@id="table5"]/tbody/tr[8]/td[2]/input').click()
    driver.find_element_by_xpath('//*[@id="table5"]/tbody/tr[9]/td[2]/input').click()
    driver.find_element_by_xpath('//*[@id="table5"]/tbody/tr[13]/td[2]/input').click()
    driver.find_element_by_xpath('//*[@id="table5"]/tbody/tr[20]/td[2]/input').click()
    print("click cities & properties")
    sleep(3)
    
    #按下下載
    driver.find_element(By.CSS_SELECTOR,"#downloadBtnId").click()
    print("download finished!")
    
def unzip1():
    #壓縮檔名稱
    file_name = "download.zip"
    #當下路徑+壓縮檔名稱
    path = os.path.join(os.getcwd(), file_name)
    #若壓縮檔尚未下載完成，等待五秒
    while file_name not in os.listdir(os.getcwd()):
        sleep(5)
        print('no')
    
    #解壓縮五個檔案至當下工作目錄
    z = zipfile.ZipFile(path, "r")
    for i in ['A_lvr_land_A.csv','B_lvr_land_A.csv','E_lvr_land_A.csv','F_lvr_land_A.csv','H_lvr_land_A.csv']:
        f = z.open(i)
        df=pd.read_csv(f)
        df.to_csv(i, index=False,encoding="utf-8_sig")
    
crawler()
unzip1()

'''
使用 Spark 讀取檔名 【 a_lvr_land_a 】【 b_lvr_land_a 】 【 e_lvr_land_a 】【f_lvr_land_a 】 【 h_lvr_land_a 】 五份資料集。

以下為第二題
讀取檔案
新增欄位'city'
'''

#SparkSQL 的入口
from pyspark.sql import SparkSession
#使用functions中 lit, col, todate
from pyspark.sql import functions as F
import numpy as np
import pandas as pd
#轉換型態
from pyspark.sql.types import StringType, IntegerType
#於spark dataframe 使用 udf
from pyspark.sql.functions import udf
#建立pandas udf
from pyspark.sql.functions import pandas_udf, PandasUDFType

#使用local mode.
spark = SparkSession.builder.appName("mldemo").getOrCreate()

#每個檔案第一列皆為英文別名，避免 spark dataframe 欄位型態皆為 string，先刪除第一列。
for i in ['A_lvr_land_A','B_lvr_land_A','E_lvr_land_A','F_lvr_land_A','H_lvr_land_A']:
    df=pd.read_csv(i+'.csv')
    df.drop(df.head(1).index, inplace=True)
    df.to_csv(i+'_drop1.csv', index=False,encoding="utf-8_sig")

#將檔案讀取成spark dataframe
dfA = spark.read.csv("A_lvr_land_A_drop1.csv",sep = ',',header = True,inferSchema = True)
dfB = spark.read.csv("B_lvr_land_A_drop1.csv",sep = ',',header = True,inferSchema = True)
dfE = spark.read.csv("E_lvr_land_A_drop1.csv",sep = ',',header = True,inferSchema = True)
dfF = spark.read.csv("F_lvr_land_A_drop1.csv",sep = ',',header = True,inferSchema = True)
dfH = spark.read.csv("H_lvr_land_A_drop1.csv",sep = ',',header = True,inferSchema = True)
dfA.printSchema()

#新增欄位'city'，用以輸出分組使用。用以lit轉換成spark理解形式
dfA = dfA.withColumn('city', F.lit('台北市'))
dfB = dfB.withColumn('city', F.lit('台中市'))
dfE = dfE.withColumn('city', F.lit('高雄市'))
dfF = dfF.withColumn('city', F.lit('新北市'))
dfH = dfH.withColumn('city', F.lit('桃園市'))
dfA.printSchema()

'''
使用 Spark 合併 資料 集， 以下列條件 從篩選出結果
【 主要用途 】 為 【 住家用 】
【 建物型態 】 為 【 住宅大樓 】
【 總樓層數 】 需 【 大於等於十三層 】

以下為第三題
合併
新增欄位'Floors'
篩選
'''

#確認欄位相同，才能使用union()合併
dfA.columns == dfB.columns == dfE.columns == dfF.columns == dfH.columns

#合併檔案(確認欄位相同)
dfall = dfA.union(dfB).union(dfE).union(dfF).union(dfH)
print(dfall.count())

#確認總數無誤
dfall.count() == dfA.count() + dfB.count() + dfE.count() + dfF.count() + dfH.count()

#'總樓層數'資料型態為 string，包含中文數字與層字。
#需要先轉換為能做數值大小比較之欄位。
#中文數字轉數值方法
def chitonum(chi):
    if chi is None:return 0
    
    #刪掉"層"
    if chi[-1] == '層':chi = chi[:-1]
    num_cor = {'一':1, '二':2, '三':3, '四':4, '五':5, '六':6, '七':7, '八':8, '九':9}
    num=0
    
    #找 "十" 位置
    t_num=chi.find('十')
    
    # -1即為沒有十，取十位數前面的中文數字，若前面沒有中文數字即為十
    if t_num >= 0:
        num  += num_cor.get(chi[t_num-1:t_num],1) * 10
    if chi[-1] in num_cor: num  += num_cor[chi[-1]]
    return num

# 建立pandas udf
# 建立一般函數使用chitonum方法
def chitonum2(chi: pd.Series) -> pd.Series:
    num_series = chi.apply(chitonum)
    return num_series

# 建立pandas_udf 使用於spark dataframe 欄位上
trans_pd_udf = pandas_udf(chitonum2, returnType = IntegerType())
dfallf=dfall.withColumn('Floors',trans_pd_udf(F.col('總樓層數')))
dfallf.printSchema()

#篩選三條件，'主要用途'為'住家用'，'建物型態'為'住宅大樓'(包含)，'總樓層數'大於等於'十三樓'，在此以轉換過欄位'Floors'篩選
dffilter = dfallf.filter((F.col('主要用途') == '住家用') &
                         (F.col('建物型態').contains('住宅大樓')) &
                         (F.col('Floors')>=13))

#確認篩選結果
dffilter.select('主要用途').distinct().show()
dffilter.select('建物型態').distinct().show()
dffilter.select('總樓層數').distinct().orderBy('總樓層數',ascending=0).show(100)

'''
使用 Spark 將步驟 3 的篩選結果，轉換成 Json 格式。
產生 【 result part1.json 】 和【result part2.json 】五個城市隨機放入兩個檔案。
•time_slots 需根據 date 作 desc 排序 越近期的放 前面
•每行各自是 valid Json 

以下為第四題
新增欄位'date'
建立多層巢狀
隨機分割
輸出
'''

#新增欄位'date'，從'交易年月日'轉換西元而成，用以輸出組合用。
#'交易年月日'數值型態，且年分皆位於第5位數，加以1911轉成西元。
#再以to_date()轉換成yyyy-MM-dd形式，注意轉換型別
dfdate = dffilter.withColumn('date', F.to_date((F.col('交易年月日') + 19110000).cast(StringType()), 'yyyyMMdd')) 
dfdate.printSchema()

#建立巢狀Json檔案前置作業。
#抓取分組用'city','date' 以及2個原始資料欄位，符合題目範例，以英文命名。
df_preresult = dfdate.select('city' , 'date',
                             F.col('鄉鎮市區').alias('district'),
                             F.col('建物型態').alias('type'),
                            )

#建立第一層巢狀
#struct 將原始資料欄位放置同一欄位以'events'命名
#groupby 以 'city', 'date' 分組將'events'聚合
#orderBy 以 'date' 由大到小排序

df_preresult_re=df_preresult.withColumn(
    "events",
    F.struct(
        F.col('district'),
        F.col('type'),
    )
).groupBy(['city', 'date']).agg(F.collect_list('events').alias('events'))\
.orderBy('date', ascending=0)
df_preresult_re.show()

#建立第二層巢狀
#struct 將 'date', 'events'放置同一欄位以 'time_slots' 命名
#groupBy 以 'city' 分組將 'time_slots' 聚合
df_result=df_preresult_re.withColumn(
  "time_slots",
  F.struct(
    F.col("date"),
    F.col("events")
  )
).groupBy('city').agg(F.collect_list('time_slots').alias('time_slots'))
df_result.show()

#隨機分割，以randomSplit 隨機分割成兩份，權重機率相同，並非對半分。
df_result1,df_result2=df_result.randomSplit([0.5,0.5])
df_result1.show()
df_result2.show()

#輸出成Json 檔案，repartition()，避免檔案分割，這邊分割並不會每次隨機。
df_result1.repartition(1).write.mode('overwrite').json('output1.json')
df_result2.repartition(1).write.mode('overwrite').json('output2.json')

spark.stop()


# In[ ]:




