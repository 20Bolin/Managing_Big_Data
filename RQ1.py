"""
The following script returns the first 20 most frequent drop-off locations of green taxi in 2019. 
For specifically extracting weekday or weekday data, just add a filter method like what the comment shows. 
If you want data from another year or of different taxt category, just change the file path.
"""

from pyspark import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import split, col, trim, explode, desc, count
from pyspark.sql.types import DoubleType

sc = SparkContext(appName = "Testing")
spark = SparkSession(sc)
sc.setLogLevel("ERROR")

#df1 = spark.read.json(sc.parallelize([parse_green]))
file_path_list = ["/path/of/my/file1", "/path/of/my/file2", ..."/path/of/my/fileN"]

#Line 19-28 is to unify "ehail_fee" in all 2019 green taxi dataset in DoubleType, or the merge of parquet files will fail.
#If all the files have the same data type on each column, you can directly merge it from the code of line 31.
dfs = []

for file_path in file_path_list:
    df = spark.read.parquet(file_path)
    df = df.withColumn("ehail_fee", col("ehail_fee").cast(DoubleType()))
    dfs.append(df)

df1 = dfs[0]
for df in dfs[1:]:
    df1 = df1.union(df)

df1 = spark.read.parquet(*file_path_list, mergeSchema=True)

#To tag whether this trip is on weekday or on weekend, very useful I have to say.
df2 = df1.withColumn("weekend", ((dayofweek("lpep_pickup_datetime")==1)|(dayofweek("lpep_pickup_datetime")==7)).cast("int"))

df3 = df1.drop("mta_tax", "improvement_surcharge", "VendorID")

#Turn the datatype of values in "dolocationid" into array so that explode method can be applied
df4 = df3.withColumn("DOLocationID", trim(col("DOLocationID"))).withColumn("DOLocationID", split(col("DOLocationID"), ","))

#df5 = df4.filter(df3.weekend == 1 or 0)

df5 = df4.select(explode("DOLocationID").alias("DOLocationID"))

df6 = df5.groupBy("DOLocationID").agg(count(col("*")).alias("count"))

df7 = df6.orderBy(desc("count")).show(20)

df8 = df7.limit(20)

df8.write.csv("/user/s3022455/Green")

"""
Green taxi 2019 example
+------------+------+
|DOLocationID| count|
+------------+------+
|          74|230014|
|          42|214976|
|          41|190330|
|          75|166268|
|         129|157254|
|           7|155140|
|         166|128331|
|         181|106210|
|          82|104244|
|         223| 96710|
|          95| 96558|
|         236| 96190|
|         244| 91202|
|         238| 90936|
|          61| 90322|
|         116| 85612|
|          97| 84012|
|          49| 72124|
|         138| 71428|
|         226| 70451|
+------------+------+
"""