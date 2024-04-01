import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StringType
import pandas as pd

# Column names
logColumns = ["logdate","logtime","size","r_version","r_arch","r_os","package","version","country","ip_id"]

# Input Data as a list of values
listInput = [
("2012-10-01","02:13:48",1061394,"2.15.1","i686","linux-gnu","Amelia","1.6.3","AU",1),
("2012-10-01","02:37:34",868687,"2.15.0","x86_64","linux-gnu","RCurl","1.95-0","US",3),
("2012-10-01","04:06:10",1023,"NA","NA","NA","NA","NA","US",4),
("2012-10-01","08:17:26",2094435,"2.15.1","x86_64","linux-gnu","mosaic","0.6-2","US",2),
("2012-10-01","08:29:01",868687,"2.15.1","x86_64","linux-gnu","RCurl","1.95-0","US",2),
("2012-10-01","08:28:54",2094449,"2.15.1","x86_64","linux-gnu","mosaic","0.6-2","US",2)
]

# Assignment1: Find Downloads by country, package using RDD API
def getDownloadsByCountry(inputRDD):
  if inputRDD != None:
    downloads = inputRDD.map( lambda x : ((x[6], x[8]),1) )
    outputRDD = downloads.reduceByKey( lambda acc,value : value + 1 )
    return outputRDD

# Assignment2: Increment Accumulator variable using RDD API
# Below function will increment the accumulator variable for every record having size > 1000000
def incLargeRecords (size):
  if (size >= 1000000):
    accDownloadCount.add(1)
def getDownloadCount(inputRDD,accDownloadCount):
  inputRDD.foreach( lambda x : incLargeRecords(int(x[2])))
  return accDownloadCount.value

# Assignment 3: Filter records
# Below function will read the input Dataframe having records in listInput and filter out records
# where version = "NA" and package = "NA"
def filterRecords(inputDF):
  outputDF = (inputDF
                 .select("logdate","logtime","size","r_version","r_arch","r_os","package","version","country","ip_id")
                 .filter("package != 'NA'")
                 .filter("version != 'NA'")
                 .orderBy("logdate","logtime")
                 )
  return outputDF

# Assignment 4: Add download_type column
# Below function will add a new column called download_type to the input dataframe
def downloadType(size):
  if size <= 1000000:
    return "small"
  else:
    return "large"
udf_downloadType = udf(downloadType, StringType())
def addDownloadType(inputDF):
  outputDF = (inputDF
                 .withColumn("download_type", udf_downloadType("size"))
                 .select("logdate", "logtime", "size", "r_version", "r_arch", "r_os", "package", "version", "country", "ip_id","download_type")
                 .orderBy("logdate","logtime")
                 )
  return outputDF

# Assignment 5: Total number of downloads for each package
def getPackageCount(inputDF):
  inputDF.createOrReplaceTempView("inputDF")
  outputDF = spark.sql("""select package, count(package) as package_count from inputDF group by package """)
  return outputDF


# Assignment 6: Aggregate input dataframe based on logdate, download_type and find total_size,average_size
def aggDownloadType(inputDF):
  inputDF.createOrReplaceTempView("inputDF")
  outputDF = spark.sql("""select sum(size) as total_size , round(avg(size),0) as average_size from inputDF group by download_type,logdate """)
  return outputDF

if __name__ == "__main__":  
  spark = SparkSession.builder.appName("PySpark Assignments").getOrCreate()
  spark.sparkContext.setLogLevel("WARN")
  
# Initialize Accumulator Variable
accDownloadCount = spark.sparkContext.accumulator(0)

inputRDD = spark.sparkContext.parallelize(listInput)
downloadsByCountryRDD = getDownloadsByCountry(inputRDD)

print("Assignment 1 Output : Downloads By Country")
print("###############################################")
print(downloadsByCountryRDD.collect())

print("Assignment 2 Output : Download Count")
print("###############################################")
downloadCount = getDownloadCount(inputRDD,accDownloadCount)
print("Total Downloads of size > 1000000 :" + str(downloadCount))

df_pandas = pd.DataFrame.from_dict(listInput)
df = spark.createDataFrame(df_pandas, logColumns)
print("Assignment 3 Output : Filter records")
print("###############################################")
df_filter = filterRecords(df)
df_filter.show()

print("Assignment 4 Output : Add DownloadType column")
print("###############################################")
df_downloadType = addDownloadType(df_filter)
df_downloadType.show()

df_package_count = getPackageCount(df_downloadType)
print("Assignment 5 Output : Count by Package")
print("###############################################")
df_package_count.show()


df_agg_download_type = aggDownloadType(df_downloadType)
print("Assignment 6 Output : Total and Average Size")
print("###############################################")
df_agg_download_type.show()