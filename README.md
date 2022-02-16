# covid api개발+pyspark으로 데이터 처리+ airflow로 스케쥴링+mysql로 data insert+csv파일로 저장하기 project

# 데이터 ETL 프로세스

1. 공공데이터 api에서 xml형태(lxml)로 데이터를 불러옴(pip install lxml)
2. 불러온 xml데이터를 list로 만들음
3. list를 스키마 입혀 pyspark dataframe으로써 생성
4. 생성된 dataframe을 입력받아 전체 데이터, 오늘 데이터, 입력받은 기간 동안의 데이터를 출력하는 함수 구현
5. pyspark dataframe을 csv파일로 저장, 기준 날짜를 기준으로 partitioned된 csv파일로 저장, mysql DB에 입력 하게 끔 하는 함수 구현(전체 데이터, 오늘 데이터, 입력받은 기간 동안의 데이터 별로 저장되는 디렉토리를 다르게 설정)
6. 오늘 데이터가 누락 되어 type에러를 발생 시킨다면, daily batch를 하지않고 안내문 출력,
7. 오늘 데이터가 누락 되지 않았다면, daily batch 수행(mysql에 데이터 저장, 파티션별로 데이터 저장, 전체 데이터 overwrite하여 저장)
8. 클라우드에서는 s3로도 보내는 구조
    1. s3로 데이터 보내기 mysql 에서 보내는것, file을 보내는 법, s3로 직접 쓰는 법
    2. s3 정책과 public access write permission 확인하기
    3. boto3
    

**api_development_xml_pysaprk.py 작성(on windows)**

```python
#스파크가 pandas보다 느리고, 스파크는 큰 데이터가 아니면 의미없지만, 스파크에서 배운 data transformation을 활용 하기위해 스파크를 채택
#with corona가 시행됨에 따라 검사수를 더 이상 측정을 안하게됨
import requests
from bs4 import BeautifulSoup
from datetime import datetime, date, timedelta
from typing import Union
import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Row
from pyspark.sql.types import StructField, StructType, DoubleType, IntegerType, TimestampType, StringType
#sparksession 드라이버 프로세스 얻기
spark = SparkSession.builder.master("local[*]").config("spark.driver.extraClassPath","C:/spark/spark-3.1.2-bin-hadoop2.7/jars/mysql-connector-java-8.0.28").appName("pyspark").getOrCreate()

now = datetime.now()
today = date.today()
oneday = timedelta(days=1)

#RESTapi로 부터 데이터를 얻어온다.
def getCovid19Info(start_date: date, end_date: date):
    url = 'http://openapi.data.go.kr/openapi/service/rest/Covid19/getCovid19InfStateJson'
    api_key_utf8 = 'mrAZG1yevJBgcaaSuVLOgJ%2BS6blzA0SXlGYZrwxwpARTaMnSotfqFooTr6dgKpPcTBtE96l0xE%2B%2BmXxDrWt19g%3D%3D'
    api_key_decode = requests.utils.unquote(api_key_utf8, encoding='utf-8')

    params ={
        'serviceKey' : api_key_decode,
        'startCreateDt' : int('{:04d}{:02d}{:02d}'.format(start_date.year, start_date.month, start_date.day)),
        'endCreateDt' : int('{:04d}{:02d}{:02d}'.format(end_date.year, end_date.month, end_date.day)),
    }

    response = requests.get(url, params=params)
    content = response.text
    elapsed_us = response.elapsed.microseconds
    print('Reqeust Done, Elapsed: {} seconds'.format(elapsed_us / 1e6))#100만
    
    return BeautifulSoup(content, "lxml")#불러온 text 데이터를 python xml인 lxml로 변환

#union을 통해 date, datetime 두개의 type모두 허용
def getCovid19SparkDataFrame(start_date: Union[date, datetime], end_date: Union[date,datetime]):
    #key값의 데이터 타입 정의
    convert_method = {
        'accdefrate' : float,
        'accexamcnt' : int,
        'createdt' : lambda x: datetime.strptime(x, '%Y-%m-%d %H:%M:%S.%f'),
        'deathcnt' : int,
        'decidecnt' : int,
        'seq' : int,
        'statedt' : str,
        'statetime' : str,
        'updatedt' : lambda x: datetime.strptime(x, '%Y-%m-%d %H:%M:%S.%f'),
    }

    temp = getCovid19Info(start_date, end_date)
    items = temp.find('items')#item만 추출
    item_list = []
    for item in items:
        item_dict = {}
        for tag in list(item):
            try:
                item_dict[tag.name] = convert_method[tag.name](tag.text)
            except Exception:
                item_dict[tag.name] = None
        item_list.append(item_dict)#lxml데이터를 dictionary in list로 변환

    #dictionary의 list를 spark DataFrame으로 바꾸는 방법 dictionary 속의 key를 schema를 정의해준다.
    CovidInfo_item_Schema = StructType([
        StructField('accdefrate', DoubleType(), True),#누적확진률
        StructField('accexamcnt', IntegerType(), True),#누적의심신고검사자
        StructField('createdt', TimestampType(),True),#등록일시분초
        StructField('deathcnt', IntegerType(), True),#사망자 수
        StructField('decidecnt', IntegerType(), True),#확진자 수
        StructField('seq', IntegerType(), True),#게시글 번호(감염현황 고유값)
        StructField('statedt', StringType(), True),#기준일
        StructField('statetime', StringType(), True),#기준시간
        StructField('updatedt', TimestampType(), True)#수정일시분초
    ])
    
    df_Covid19 = spark.createDataFrame(item_list,CovidInfo_item_Schema)
    return df_Covid19

#XML로 부터 파싱된 item들의 모든 데이터를 리턴
#데이터 조회시 메소드.show() ex)getAllCovid19Data().show()
def getAllCovid19Data():
    df_Covid19 = getCovid19SparkDataFrame(date(2019,1,1), now)
    print("Today: {}\nLoaded {} Records".format(now.strftime('%Y-%m-%d'),df_Covid19.distinct().count()))
    return df_Covid19

#코로나 매일의 확진자, 누적확진자, 사망자, 누적사망자, 치명률, 검사자,누적검사자를 뽑아낸 dataframe
#임시 선별 검사자 수는 제외(api를 못구함)
#데이터 조회시 메소드.show() ex)getAllCovidAffectedNumbers().show()
def getAllCovidAffectedNumbers():
    from pyspark.sql import Window
    df_Covid19 =  getCovid19SparkDataFrame(date(2019,1,1), now)
    df_Covid19_result_All=\
    df_Covid19.distinct()\
              .withColumn("decidecnt-1",F.coalesce(F.lead(F.col("decidecnt"),1).over(Window.orderBy(F.col("statedt").desc())),F.lit(0)))\
              .withColumn("deathcnt-1",F.coalesce(F.lead(F.col("deathcnt"),1).over(Window.orderBy(F.col("statedt").desc())),F.lit(0)))\
              .withColumn("accexamcnt-1",F.coalesce(F.lead(F.col("accexamcnt"),1).over(Window.orderBy(F.col("statedt").desc())),F.lit(0)))\
              .withColumn("기준날짜",F.from_unixtime(F.unix_timestamp(F.col("statedt"),"yyyyMMdd"),"yyyy-MM-dd"))\
              .select(F.col("기준날짜")
                     ,(F.col("decidecnt")-F.col("decidecnt-1")).alias("당일확진자수")
                     ,F.col("decidecnt").alias("누적확진자수")
                     ,(F.col("deathcnt")-F.col("deathcnt-1")).alias("당일사망자수")
                     ,F.col("deathcnt").alias("누적사망자수")
                     ,(F.col("accexamcnt")-F.col("accexamcnt-1")).alias("당일의심신고검사자수")
                     ,F.col("accexamcnt").alias("누적의심신고검사자수")
                     ,F.round((F.col("deathcnt")/F.col("decidecnt")*F.lit(100)),2).alias("전체확진자치명률(%)"))
    return df_Covid19_result_All

#코로나 오늘의 확진자, 누적확진자, 사망자, 누적사망자, 치명률, 검사자,누적검사자를 뽑아낸 dataframe
#임시 선별 검사자 수는 제외(api를 못구함)
#데이터 조회시 메소드.show() ex)getAllCovidAffectedNumbers().show()    
def getTodayCovidAffectedNumbers():
    from pyspark.sql import Window
    now_date=now.strftime('%Y-%m-%d')
    df_Covid19 =  getCovid19SparkDataFrame(date(2019,1,1), now)
    df_Covid19_result_Today=\
    df_Covid19.distinct()\
              .withColumn("decidecnt-1",F.coalesce(F.lead(F.col("decidecnt"),1).over(Window.orderBy(F.col("statedt").desc())),F.lit(0)))\
              .withColumn("deathcnt-1",F.coalesce(F.lead(F.col("deathcnt"),1).over(Window.orderBy(F.col("statedt").desc())),F.lit(0)))\
              .withColumn("accexamcnt-1",F.coalesce(F.lead(F.col("accexamcnt"),1).over(Window.orderBy(F.col("statedt").desc())),F.lit(0)))\
              .withColumn("기준날짜",F.from_unixtime(F.unix_timestamp(F.col("statedt"),"yyyyMMdd"),"yyyy-MM-dd"))\
              .where(F.col("기준날짜")==now_date)\
              .select(F.col("기준날짜")
                     ,(F.col("decidecnt")-F.col("decidecnt-1")).alias("당일확진자수")
                     ,F.col("decidecnt").alias("누적확진자수")
                     ,(F.col("deathcnt")-F.col("deathcnt-1")).alias("당일사망자수")
                     ,F.col("deathcnt").alias("누적사망자수")
                     ,(F.col("accexamcnt")-F.col("accexamcnt-1")).alias("당일의심신고검사자수")
                     ,F.col("accexamcnt").alias("누적의심신고검사자수")
                     ,F.round((F.col("deathcnt")/F.col("decidecnt")*F.lit(100)),2).alias("전체확진자치명률(%)"))
    return df_Covid19_result_Today

#코로나 범위내의 확진자, 누적확진자, 사망자, 누적사망자, 치명률, 검사자,누적검사자를 뽑아낸 dataframe
#임시 선별 검사자 수는 제외(api를 못구함)
#데이터 조회시 메소드.show() ex)getAllCovidAffectedNumbers().show()
def getPeriodCovidAffectedNumbers(start_date: Union[date, datetime], end_date: Union[date,datetime]):
    from pyspark.sql import Window
    df_Covid19 =  getCovid19SparkDataFrame(date(2019,1,1), now)
    df_Covid19_result_Period=\
    df_Covid19.distinct()\
              .withColumn("decidecnt-1",F.coalesce(F.lead(F.col("decidecnt"),1).over(Window.orderBy(F.col("statedt").desc())),F.lit(0)))\
              .withColumn("deathcnt-1",F.coalesce(F.lead(F.col("deathcnt"),1).over(Window.orderBy(F.col("statedt").desc())),F.lit(0)))\
              .withColumn("accexamcnt-1",F.coalesce(F.lead(F.col("accexamcnt"),1).over(Window.orderBy(F.col("statedt").desc())),F.lit(0)))\
              .withColumn("기준날짜",F.from_unixtime(F.unix_timestamp(F.col("statedt"),"yyyyMMdd"),"yyyy-MM-dd"))\
              .where(F.col("기준날짜").between(start_date,end_date))\
              .select(F.col("기준날짜")
                     ,(F.col("decidecnt")-F.col("decidecnt-1")).alias("당일확진자수")
                     ,F.col("decidecnt").alias("누적확진자수")
                     ,(F.col("deathcnt")-F.col("deathcnt-1")).alias("당일사망자수")
                     ,F.col("deathcnt").alias("누적사망자수")
                     ,(F.col("accexamcnt")-F.col("accexamcnt-1")).alias("당일의심신고검사자수")
                     ,F.col("accexamcnt").alias("누적의심신고검사자수")
                     ,F.round((F.col("deathcnt")/F.col("decidecnt")*F.lit(100)),2).alias("전체확진자치명률(%)"))
    return df_Covid19_result_Period, start_date, end_date#이후에 함수별로 저장할 때, DF가 기간으로 입력받은 것을 알기위해 기간값도 리턴

#위의 3개의 메소드를 통해 데이터프레임을 입력받아 c드라이브 covid19_result 폴더에 함수 형태에 따른csv파일로 저장

def saveDataAsCSV(dataframe):
    if type(dataframe) == tuple:
        dataframe[0].cache()
        start_date=dataframe[1]
        end_date=dataframe[2]
        if dataframe[0].count() == getPeriodCovidAffectedNumbers(start_date, end_date)[0].count():
            print("it worked!")
            return dataframe[0].coalesce(1).write.format("csv").option("header","true").mode("overwrite").save("C:/covid19_result/period/")
    else:
        dataframe.cache()
        if dataframe.count() == getAllCovidAffectedNumbers().count():
            print("it worked!")
            return dataframe.coalesce(1).write.format("csv").option("header","true").mode("overwrite").save("C:/covid19_result/all_day/")
        elif dataframe.count() == getTodayCovidAffectedNumbers().count():
            print("it worked!")
            return dataframe.coalesce(1).write.format("csv").option("header","true").mode("overwrite").save("C:/covid19_result/today/")

#파티션별로 나누어 파일 저장(daily partition별로 데이터 적재)        
def saveDataAsPartitionCSV(dataframe):
    if type(dataframe) == tuple:
        dataframe[0].cache()
        start_date=dataframe[1]
        end_date=dataframe[2]
        if dataframe[0].count() == getPeriodCovidAffectedNumbers(start_date, end_date)[0].count():
            print("it worked!")
            return dataframe[0].write.format("csv").option("header","true").mode("overwrite").partitionBy("기준날짜").save("C:/covid19_result/partition/period/")
    else:
        dataframe.cache()
        if dataframe.count() == getAllCovidAffectedNumbers().count():
            print("it worked!")
            return dataframe.write.format("csv").option("header","true").mode("overwrite").partitionBy("기준날짜").save("C:/covid19_result/partition/all_day/")
        elif dataframe.count() == getTodayCovidAffectedNumbers().count():
            print("it worked!")
            return dataframe.write.format("csv").option("header","true").mode("overwrite").partitionBy("기준날짜").save("C:/covid19_result/partition/today/")

#mysql DB에 Covid19 DataFrame의 데이터를 저장
def saveDataToMySQL(dataframe):
    if type(dataframe) == tuple:
        dataframe[0].cache()
    else:
        dataframe.cache()
        
    if dataframe.count() == getAllCovidAffectedNumbers().count():
        print("insert to mysql Covid19 table")
        return dataframe.coalesce(1).write.format("jdbc").options(
            url='jdbc:mysql://localhost:3306/COVID19',
            driver='com.mysql.cj.jdbc.Driver',
            dbtable='Covid_19_info',
            user='root',
            password='root'
        ).mode('overwrite').save()
    
if __name__=="__main__":
    try:
        today_infection_num = (getTodayCovidAffectedNumbers().first()["당일확진자수"])
        infection_num_diff = (getTodayCovidAffectedNumbers().first()["당일확진자수"] - 
                                                     getPeriodCovidAffectedNumbers(today-oneday,today)[0].where(F.col("기준날짜")==str(today-oneday)).first()["당일확진자수"])
        print("오늘(%s)의 확진자수는 %d명입니다.\n" % (today, today_infection_num))#df.first()['column name'] 혹은 df.collect()[0]['column name'], 오늘의 데이터가 없을 경우 none type이 되어 에러를 낸다.try except 처리
        if infection_num_diff >= 0:
            print("어제보다 코로나 확진자가 %d명 늘었습니다.\n" % (infection_num_diff))
        else:
            print("어제보다 코로나 확진자가 %d명 줄었습니다.\n" % (-infection_num_diff))
    except TypeError:
        print("오늘의 데이터가 아직 입력되지 않았습니다.")
    
    saveDataAsCSV(getAllCovidAffectedNumbers())
    saveDataAsPartitionCSV(getAllCovidAffectedNumbers())
    saveDataToMySQL(getAllCovidAffectedNumbers())
```

**api_development_xml_pysaprk.py 작성(on ec2)**

```python
#스파크가 pandas보다 느리고, 스파크는 큰 데이터가 아니면 의미없지만, 스파크에서 배운 data transformation을 활용 하기위해 스파크를 채택
import requests
from bs4 import BeautifulSoup
from datetime import datetime, date, timedelta
from typing import Union
import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Row
from pyspark.sql.types import StructField, StructType, DoubleType, IntegerType, TimestampType, StringType
#sparksession 드라이버 프로세스 얻기
spark = SparkSession.builder.master("local[*]").config("spark.driver.extraClassPath","/home/ubuntu/mysql-connector-java-8.0.28/mysql-connector-java-8.0.28.jar").appName("pyspark").getOrCreate()

now = datetime.now()
today = date.today()
oneday = timedelta(days=1)

def getCovid19Info(start_date: date, end_date: date):
    url = 'http://openapi.data.go.kr/openapi/service/rest/Covid19/getCovid19InfStateJson'
    api_key_utf8 = 'mrAZG1yevJBgcaaSuVLOgJ%2BS6blzA0SXlGYZrwxwpARTaMnSotfqFooTr6dgKpPcTBtE96l0xE%2B%2BmXxDrWt19g%3D%3D'
    api_key_decode = requests.utils.unquote(api_key_utf8, encoding='utf-8')

    params ={
        'serviceKey' : api_key_decode,
        'startCreateDt' : int('{:04d}{:02d}{:02d}'.format(start_date.year, start_date.month, start_date.day)),
        'endCreateDt' : int('{:04d}{:02d}{:02d}'.format(end_date.year, end_date.month, end_date.day)),
    }

    response = requests.get(url, params=params)
    content = response.text
    elapsed_us = response.elapsed.microseconds
    print('Reqeust Done, Elapsed: {} seconds'.format(elapsed_us / 1e6))#100만
    
    return BeautifulSoup(content, "lxml")#python xml인 lxml로 변환

#union을 통해 date, datetime 두개의 type모두 허용
def getCovid19SparkDataFrame(start_date: Union[date, datetime], end_date: Union[date,datetime]):
    #key값의 데이터 타입 정의
    convert_method = {
        'accdefrate' : float,
        'accexamcnt' : int,
        'createdt' : lambda x: datetime.strptime(x, '%Y-%m-%d %H:%M:%S.%f'),
        'deathcnt' : int,
        'decidecnt' : int,
        'seq' : int,
        'statedt' : str,
        'statetime' : str,
        'updatedt' : lambda x: datetime.strptime(x, '%Y-%m-%d %H:%M:%S.%f'),
    }
    #파싱 후 dictionay들의 list 형태 변환 과정 이해하기
    temp = getCovid19Info(start_date, end_date)
    items = temp.find('items')
    item_list = []
    for item in items:
        item_dict = {}
        for tag in list(item):
            try:
                item_dict[tag.name] = convert_method[tag.name](tag.text)
            except Exception:
                item_dict[tag.name] = None
        item_list.append(item_dict)

    #dictionary의 list를 spark DataFrame으로 바꾸는 방법 dictionary 속의 key를 schema를 정의해준다.
    CovidInfo_item_Schema = StructType([
        StructField('accdefrate', DoubleType(), True),#누적확진률
        StructField('accexamcnt', IntegerType(), True),#누적의심신고검사자
        StructField('createdt', TimestampType(),True),#등록일시분초
        StructField('deathcnt', IntegerType(), True),#사망자 수
        StructField('decidecnt', IntegerType(), True),#확진자 수
        StructField('seq', IntegerType(), True),#게시글 번호(감염현황 고유값)
        StructField('statedt', StringType(), True),#기준일
        StructField('statetime', StringType(), True),#기준시간
        StructField('updatedt', TimestampType(), True)#수정일시분초
    ])
    
    df_Covid19 = spark.createDataFrame(item_list,CovidInfo_item_Schema)
    return df_Covid19

#XML로 부터 파싱된 item들의 모든 데이터를 리턴
#데이터 조회시 메소드.show() ex)getAllCovid19Data().show()
def getAllCovid19Data():
    df_Covid19 = getCovid19SparkDataFrame(date(2019,1,1), now)
    print("Today: {}\nLoaded {} Records".format(now.strftime('%Y-%m-%d'),df_Covid19.distinct().count()))
    return df_Covid19

#코로나 매일의 확진자, 누적확진자, 사망자, 누적사망자, 치명률, 검사자,누적검사자를 뽑아낸 dataframe
#임시 선별 검사자 수는 제외(api를 못구함)
#데이터 조회시 메소드.show() ex)getAllCovidAffectedNumbers().show()
def getAllCovidAffectedNumbers():
    from pyspark.sql import Window
    df_Covid19 =  getCovid19SparkDataFrame(date(2019,1,1), now)
    df_Covid19_result_All=\
    df_Covid19.distinct()\
              .withColumn("decidecnt-1",F.coalesce(F.lead(F.col("decidecnt"),1).over(Window.orderBy(F.col("statedt").desc())),F.lit(0)))\
              .withColumn("deathcnt-1",F.coalesce(F.lead(F.col("deathcnt"),1).over(Window.orderBy(F.col("statedt").desc())),F.lit(0)))\
              .withColumn("accexamcnt-1",F.coalesce(F.lead(F.col("accexamcnt"),1).over(Window.orderBy(F.col("statedt").desc())),F.lit(0)))\
              .withColumn("기준날짜",F.from_unixtime(F.unix_timestamp(F.col("statedt"),"yyyyMMdd"),"yyyy-MM-dd"))\
              .select(F.col("기준날짜")
                     ,(F.col("decidecnt")-F.col("decidecnt-1")).alias("당일확진자수")
                     ,F.col("decidecnt").alias("누적확진자수")
                     ,(F.col("deathcnt")-F.col("deathcnt-1")).alias("당일사망자수")
                     ,F.col("deathcnt").alias("누적사망자수")
                     ,(F.col("accexamcnt")-F.col("accexamcnt-1")).alias("당일의심신고검사자수")
                     ,F.col("accexamcnt").alias("누적의심신고검사자수")
                     ,F.round((F.col("deathcnt")/F.col("decidecnt")*F.lit(100)),2).alias("전체확진자치명률(%)"))
    return df_Covid19_result_All

#코로나 오늘의 확진자, 누적확진자, 사망자, 누적사망자, 치명률, 검사자,누적검사자를 뽑아낸 dataframe
#임시 선별 검사자 수는 제외(api를 못구함)
#데이터 조회시 메소드.show() ex)getAllCovidAffectedNumbers().show()    
def getTodayCovidAffectedNumbers():
    from pyspark.sql import Window
    now_date=now.strftime('%Y-%m-%d')
    df_Covid19 =  getCovid19SparkDataFrame(date(2019,1,1), now)
    df_Covid19_result_Today=\
    df_Covid19.distinct()\
              .withColumn("decidecnt-1",F.coalesce(F.lead(F.col("decidecnt"),1).over(Window.orderBy(F.col("statedt").desc())),F.lit(0)))\
              .withColumn("deathcnt-1",F.coalesce(F.lead(F.col("deathcnt"),1).over(Window.orderBy(F.col("statedt").desc())),F.lit(0)))\
              .withColumn("accexamcnt-1",F.coalesce(F.lead(F.col("accexamcnt"),1).over(Window.orderBy(F.col("statedt").desc())),F.lit(0)))\
              .withColumn("기준날짜",F.from_unixtime(F.unix_timestamp(F.col("statedt"),"yyyyMMdd"),"yyyy-MM-dd"))\
              .where(F.col("기준날짜")==now_date)\
              .select(F.col("기준날짜")
                     ,(F.col("decidecnt")-F.col("decidecnt-1")).alias("당일확진자수")
                     ,F.col("decidecnt").alias("누적확진자수")
                     ,(F.col("deathcnt")-F.col("deathcnt-1")).alias("당일사망자수")
                     ,F.col("deathcnt").alias("누적사망자수")
                     ,(F.col("accexamcnt")-F.col("accexamcnt-1")).alias("당일의심신고검사자수")
                     ,F.col("accexamcnt").alias("누적의심신고검사자수")
                     ,F.round((F.col("deathcnt")/F.col("decidecnt")*F.lit(100)),2).alias("전체확진자치명률(%)"))
    return df_Covid19_result_Today

#코로나 범위내의 확진자, 누적확진자, 사망자, 누적사망자, 치명률, 검사자,누적검사자를 뽑아낸 dataframe
#임시 선별 검사자 수는 제외(api를 못구함)
#데이터 조회시 메소드.show() ex)getAllCovidAffectedNumbers().show()
def getPeriodCovidAffectedNumbers(start_date: Union[date, datetime], end_date: Union[date,datetime]):
    from pyspark.sql import Window
    df_Covid19 =  getCovid19SparkDataFrame(date(2019,1,1), now)
    df_Covid19_result_Period=\
    df_Covid19.distinct()\
              .withColumn("decidecnt-1",F.coalesce(F.lead(F.col("decidecnt"),1).over(Window.orderBy(F.col("statedt").desc())),F.lit(0)))\
              .withColumn("deathcnt-1",F.coalesce(F.lead(F.col("deathcnt"),1).over(Window.orderBy(F.col("statedt").desc())),F.lit(0)))\
              .withColumn("accexamcnt-1",F.coalesce(F.lead(F.col("accexamcnt"),1).over(Window.orderBy(F.col("statedt").desc())),F.lit(0)))\
              .withColumn("기준날짜",F.from_unixtime(F.unix_timestamp(F.col("statedt"),"yyyyMMdd"),"yyyy-MM-dd"))\
              .where(F.col("기준날짜").between(start_date,end_date))\
              .select(F.col("기준날짜")
                     ,(F.col("decidecnt")-F.col("decidecnt-1")).alias("당일확진자수")
                     ,F.col("decidecnt").alias("누적확진자수")
                     ,(F.col("deathcnt")-F.col("deathcnt-1")).alias("당일사망자수")
                     ,F.col("deathcnt").alias("누적사망자수")
                     ,(F.col("accexamcnt")-F.col("accexamcnt-1")).alias("당일의심신고검사자수")
                     ,F.col("accexamcnt").alias("누적의심신고검사자수")
                     ,F.round((F.col("deathcnt")/F.col("decidecnt")*F.lit(100)),2).alias("전체확진자치명률(%)"))
    return df_Covid19_result_Period, start_date, end_date

#위의 3개의 메소드를 통해 데이터프레임을 입력받아 c드라이브 covid19_result 폴더에 함수 형태에 따른csv파일로 저장
#함수 내의 매개변수를 인식 시키는 법 찾기, class를 통해 구현, method function 구분해서 완성시키기
# count() takes exactly one argument (0 given) 해결하기
def saveDataAsCSV(dataframe):
    if type(dataframe) == tuple:
        dataframe[0].cache()
        start_date=dataframe[1]
        end_date=dataframe[2]
        if dataframe[0].count() == getPeriodCovidAffectedNumbers(start_date, end_date)[0].count():
            print("it worked!")
            return dataframe[0].coalesce(1).write.format("csv").option("header","true").mode("overwrite").save("/home/ubuntu/covid19_result/period/")
    else:
        dataframe.cache()
        if dataframe.count() == getAllCovidAffectedNumbers().count():
            print("it worked!")
            return dataframe.coalesce(1).write.format("csv").option("header","true").mode("overwrite").save("/home/ubuntu/covid19_result/all_day/")
        elif dataframe.count() == getTodayCovidAffectedNumbers().count():
            print("it worked!")
            return dataframe.coalesce(1).write.format("csv").option("header","true").mode("overwrite").save("/home/ubuntu/covid19_result/today/")
        
def saveDataAsPartitionCSV(dataframe):
    if type(dataframe) == tuple:
        dataframe[0].cache()
        start_date=dataframe[1]
        end_date=dataframe[2]
        if dataframe[0].count() == getPeriodCovidAffectedNumbers(start_date, end_date)[0].count():
            print("it worked!")
            return dataframe[0].write.format("csv").option("header","true").mode("overwrite").partitionBy("기준날짜").save("/home/ubuntu/covid19_result/partition/period/")
    else:
        dataframe.cache()
        if dataframe.count() == getAllCovidAffectedNumbers().count():
            print("it worked!")
            return dataframe.write.format("csv").option("header","true").mode("overwrite").partitionBy("기준날짜").save("/home/ubuntu/covid19_result/partition/all_day/")
        elif dataframe.count() == getTodayCovidAffectedNumbers().count():
            print("it worked!")
            return dataframe.write.format("csv").option("header","true").mode("append").partitionBy("기준날짜").save("/home/ubuntu/covid19_result/partition/today/")

def saveDataToMySQL(dataframe):
    if type(dataframe) == tuple:
        dataframe[0].cache()
    else:
        dataframe.cache()
        
    if dataframe.count() == getAllCovidAffectedNumbers().count():
        print("insert to mysql Covid19 table")
        return dataframe.coalesce(1).write.format("jdbc").options(
            url='jdbc:mysql://localhost:3306/COVID19',
            driver='com.mysql.cj.jdbc.Driver',
            dbtable='Covid_19_info',
            user='root',
            password='root'
        ).mode('overwrite').save()
    
if __name__=="__main__":
    try:
        today_infection_num = (getTodayCovidAffectedNumbers().first()["당일확진자수"])
        infection_num_diff = (getTodayCovidAffectedNumbers().first()["당일확진자수"] - 
                                                     getPeriodCovidAffectedNumbers(today-oneday,today)[0].where(F.col("기준날짜")==str(today-oneday)).first()["당일확진자수"])
        print("오늘(%s)의 확진자수는 %d명입니다.\n" % (today, today_infection_num))#df.first()['column name'] 혹은 df.collect()[0]['column name'], 오늘의 데이터가 없을 경우 none type이 되어 에러를 낸다.try except 처리
        if infection_num_diff >= 0:
            print("어제보다 코로나 확진자가 %d명 늘었습니다.\n" % (infection_num_diff))
        else:
            print("어제보다 코로나 확진자가 %d명 줄었습니다.\n" % (-infection_num_diff))
    except TypeError:
        print("오늘의 데이터가 아직 입력되지 않았습니다.")
    
    saveDataAsCSV(getAllCovidAffectedNumbers())
    saveDataAsPartitionCSV(getAllCovidAffectedNumbers())
    saveDataToMySQL(getAllCovidAffectedNumbers())
```

**airflow DAG on EC2**

```python
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'spidyweb',
    'retries': 0,
    'retry_delay': timedelta(seconds=20),
    'depends_on_past': False
}

dag_Spark_api = DAG(
    'dag_Spark_api_id',
    start_date=days_ago(2),
    default_args=default_args,
    schedule_interval='0 1 * * *',
    catchup=False,
    is_paused_upon_creation=False,
)

cmd="python3 /home/ubuntu/api_development_xml_pyspark.py"
cmd2="echo 'copying api_result files to S3' && aws s3 sync /home/ubuntu/covid19_result/partition/all_day/ s3://api-pyspark-airflow-1/api_results_partitioned/ && aws s3 sync /home/ubuntu/covid19_result/all_day/ s3://api-pyspark-airflow-1/api_results_onefile/"
cmd3="echo 'copying airflow log files to S3' && aws s3 sync /home/ubuntu/airflow/logs/dag_Spark_api_batch/spark_api_xml/ s3://api-pyspark-airflow-1/api_airflow_logs/"
cmd4="echo ‘this server will be terminated after 7minutes’ && sleep 7m && aws ec2 terminate-instances  --instance-id $(ec2metadata --instance-id)"

#시작을 알리는 dummy
task_start = DummyOperator(
    task_id='start',
    dag=dag_Spark_api,
)

#시작이 끝나고 다음단계로 진행되었음을 나타내는 dummy
task_next = DummyOperator(
    task_id='next',
    trigger_rule='all_success',
    dag=dag_Spark_api,
)
#끝을 알리는 dummy
task_finish = DummyOperator(
    task_id='finish',
    trigger_rule='all_success',
    dag=dag_Spark_api,
)

#오늘의 목표인 bash를 통해 python file 실행시킬 BashOperator
api_PySpark_1 = BashOperator(
    task_id='spark_api_xml',
    dag=dag_Spark_api,
    trigger_rule='all_success',
    bash_command=cmd,
)

#생성된 spark 결과물 파일을 s3로 복사하는 BashOperator
task_Copy_results_to_S3 = BashOperator(
    task_id='copy_results',
    dag=dag_Spark_api,
    trigger_rule='all_success',
    bash_command=cmd2,
)

#airflow log files 를 s3로 보내는 BashOperator
task_Copy_Log_to_S3 = BashOperator(
    task_id='copy_log',
    dag=dag_Spark_api,
    trigger_rule='all_success',
    bash_command=cmd3,
)

#ec2를 종료시키는 BashOperator
task_terminate_server = BashOperator(
    task_id='terminate_server',
    dag=dag_Spark_api,
    trigger_rule='all_success',
    bash_command=cmd4,
)


#의존관계 구성
task_start >> task_next >> api_PySpark_1 >> task_Copy_results_to_S3 >>  task_Copy_Log_to_S3 >> task_finish >> task_terminate_server
```

---

# **ubuntu 환경(cloud 환경)**

**리눅스 시스템 시간 now() 고치기** 

```bash
sudo apt-get install rdate
sudo rdate -s time.bora.net
```

**com.cj.mysql connector 다운 받고, mysql에 저장될 때 경로 설정**

```bash
wget https://dev.mysql.com/get/Downloads/Connector-J/mysql-connector-java-8.0.28.zip
sudo apt install unzip
unzip mysql-connector-java-8.0.28.zip
```

**bs4, lxml,findspark 설치**

```bash
pip install bs4, lxml, findspark
```

**ec2 security group**

8080포트 열어서 airflow확인 할 수있게 securitecho ‘this server will be terminated after 5minutes’ && sleep 5m && aws ec2 terminate-instances  --instance-id $EC2ID

airflow 를 통해 제출시 $EC2ID가 인식이 안된다?

echo 로 인식해보기

안될 시 (ec2metadata --instance-id)로 사용해보기 에러난다

 group tcp 8080포트 열기

**EC2 자동 배포 및 자동 끄기**

1. EC2 image생성하여 EC2 image builder, image pipeline을 이용하여 daily하게 띄움
2. aws cli를 ec2에 설치
3. ec2 instance id를 EC2ID 변수로써 .profile에 저장 (airflow에도 변수 등록?)
    1. EC2ID=$(ec2metadata --instance-id)
    2. /home/ubuntu/.local/bin/airflow variables set EC2ID $EC2ID 로 airflow에 전역 변수값을 등록 그럼 매번 켜질 때마다 등록은 어떻게 하지? → $(ec2metadata  --instance-id)처럼 줄까→일단 이 방법이 성공
4. airflow를 통한 ETL job이 다 끝나고 5분 뒤에 aws cli를 통해 ec2 terminate job 실행(AWS configure로 access KEY secret ACCESS KEY, region 입력되어야 함
    1. echo ‘this server will be terminated after 5minutes’ && sleep 5m && aws ec2 terminate-instances  --instance-id $EC2ID
    
    <aside>
    📟 spot instance로 띄우기(없는 거 같다 ㅠㅠ)3
    
    </aside>
    
    ![Untitled](https://s3-us-west-2.amazonaws.com/secure.notion-static.com/89bb8e54-1716-493f-b3c7-d1d0b51cf08e/Untitled.png)
    
    `Task received SIGTERM signal` 오류가 뜬다 →어떻게 해결? trigger_rule을 all_success로 변경?
    
    ---
    

**MySQL 설치**

```bash
sudo apt-get update
sudo apt-get install mysql-server
sudo systemctl start mysql
```

**MYSQL 시작할 때 자동 실행**

```bash
sudo systemctl enable mysql
```

**DB table생성**

```sql
create database COVID19;

use COVID19;

create table Covid_19_info(
`기준날짜` date,
`당일확진자수` int,
`누적확진자수` int,
`당일사망자수` int,
`누적사망자수` int,
`당일의심신고검사자수` int,
`누적의심신고검사자수` int,
`전체확진자치명률(%)` float,
PRIMARY KEY(기준날짜)
);
```

**sudo 없이 로그인**

```sql
ALTER USER 'root'@'localhost' IDENTIFIED WITH mysql_native_password BY '비밀번호';

FLUSH PRIVILEGES;

GRANT ALL PRIVILEGES ON COVID19.* TO 'root'@'localhost';
```

---

**spark 다운로드 및 설정**

내 tistoryblog 참조

---

# **airflow 설정**

**ec2 안에서 매일 배치처럼 돌려면 켜졌다 꺼지는 환경에서 schedule interval은 어떻게 설정해야되는가?**

1. daily
2. @once once하면 안돈다.

sudo apt-get update

s**udo apt install python3-pip**

pip3 install apache-airflow

export AIRFLOW_HOME=/home/ubuntu/airflow

./airflow db init

cd .local/bin

mkdir dags

./airflow users create \

--username admin \

--firstname Admin \

--lastname spidyweb \

--role Admin \

--email admin@spidyweb.com

systemd에 등록

아직 시작 x

dags에 파일 작성

aws cli기반으로 변수설정 하고,aws configure이후에 끄는 명령어 실행

airflow로 shell script 통해 aws ec2 terminate-instances --instance-id $EC2ID 먹히는지 확인하기(2022-02-10) 

**systemd 에 airflow-webserver.service airflow-scheduler.service 등록하기**

db를 아예 mysql로 바꿔서 진행해보기

잘 안되는 로그 보기

journalctl _PID=’프로세스아이디’

설정자체는 2개다 맞는거고 띄워지기도하는데 스케쥴된dag가 db를 참조 못하는 느낌?

```bash
[Unit]
Description=Airflow webserver
After=network.target mysql.service
wants=mysql.service

[Service]
Environment="PATH=/home/ubuntu/.local/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
User=ubuntu
Group=ubuntu
Type=simple
ExecStart=/home/ubuntu/.local/bin/airflow webserver -p 8080
Restart=on-failure
RestartSec=10s

[Install]
WantedBy=multi-user.target
```

```bash
[Unit]
Description=Airflow webserver
After=network.target mysql.service
wants=mysql.service

[Service]
Environment="PATH=/home/ubuntu/.local/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
User=ubuntu
Group=ubuntu
Type=simple
ExecStart=/home/ubuntu/.local/bin/airflow scheduler
Restart=on-failure
RestartSec=10s

[Install]
WantedBy=multi-user.target
```

---
**S3로 파일을 복사하기 단일 파일, partition된 파일 복사**

- **디렉토리 복사**

aws s3 sync /home/ubuntu/covid19_result/partition/all_day/ s3://api-pyspark-airflow-1/api_results_partitioned/

- **파일 복사**

aws s3 sync /home/ubuntu/covid19_result/all_day/ s3://api-pyspark-airflow-1/api_results_onefile/

- **로그 파일 복사**

aws s3 sync /home/ubuntu/airflow/logs/dag_Spark_api_id/spark_api_xml/ s3://api-pyspark-airflow-1/api_airflow_logs/

---

**테스트환경 순서(완성)**

로컬 jupyter→ local vm ubuntu → amazon ec2 ubuntu

---

**ubuntu terminal에서 수행 가능 하게 만들기(완성)**

터미널에서 name == main으로 터미널에서 수행 가능하게 끔 구현

---

**데이터 수집, 처리 성능(완성)**

데이터 여러번 scan하는 과정 줄이기

pyspark cached 활용 혹은api데이터 받은 것을 변수로써 저장하여 한번만 호출 하게 끔 하기

76.6초 cached 8core

87초 non cahced 8core

---

**cache()**

- cache는 함수 호출부터 실행 끝날 때 까지만 유지되는가? →  o? 맞는지 나중에 수행
- cache는 인자로 받은 dataframe으로 cache하면 함수 호출 형태로 dataframe을 불러와도 같은 것으로 인식하지 않아 cache의 효과를 보지 못하는가?→ == 처럼 boolean으로 표현해볼 시 false로 나온다.
- cache()를 해도 count()에는 영향을 주지 않는다? → x cache는 lazy execution이라 count때 첫 호출되어서 save때 영향을 받은 것
- 함수().cache()는 cache가 되지 않는다.

---

**추후에 도전해볼 과제(미완성)**

- json normalize json flatten parquet변환 (xml이아닌 json으로써 데이터 다루기)
- json 밑 dictionary형태로 mongodb저장하기
- lambda와 cloudwatch로 ec2 자동으로 띄우고 내리기
- boto3로ec2띄우고 내리기(with airflow?)
- airflow 변수 인식 argparse? 아니면 airflow variable에 변수 등록가능?

---

**더 알아보고 정리해야 될 것**

- xml vs lxml 정확한 차이? 활용도
- venv의 이점과 환경 변수가 어떻게 적용되는 지, airflow가 venv안에 설치된다면?
- boto3
- 왜 saveDataAsCSV(getTodayCovidAffectedNumbers())는 3번을 데이터 불러오는지 찾아내기
- cache 이후엔 release??
- EMR에서 s3에 파일도 마찬가지로 access, secret, jar 파일 2개?
- airflow에서 자체적으로 s3에 로그파일 생성시키는 법
