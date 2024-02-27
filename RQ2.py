"""
This script can output the aggregate rides of a certain type of taxi in a certain timeslot of one day within a certain year(so each hour is one timeslot and total 24 timeslots).
Here the example is HVFHV in the year of 2023. And the data of 2023 is updated until only October.
"""

from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import col, concat, lit, expr,format_string, hour, asc,input_file_name

sc = SparkContext(appName = 'rqProject')
sc.setLogLevel("ERROR")
spark = SparkSession(sc)

#for Green Data: columnsToKeep = ['VendorID','lpep_pickup_datetime','lpep_dropoff_datetime']
columnsToKeep = ['hvfhs_license_num','dispatching_base_num', 'request_datetime','pickup_datetime']
hdfs_directory_path = '/user/s3022455/'

file_paths = [
    hdfs_directory_path + 'taxiData/HVFHV/2023/fhvhv_tripdata_2023-01.parquet',
    hdfs_directory_path + 'taxiData/HVFHV/2023/fhvhv_tripdata_2023-02.parquet',
    hdfs_directory_path + 'taxiData/HVFHV/2023/fhvhv_tripdata_2023-03.parquet',
    hdfs_directory_path + 'taxiData/HVFHV/2023/fhvhv_tripdata_2023-04.parquet',
    hdfs_directory_path + 'taxiData/HVFHV/2023/fhvhv_tripdata_2023-05.parquet',
    hdfs_directory_path + 'taxiData/HVFHV/2023/fhvhv_tripdata_2023-06.parquet',
    hdfs_directory_path + 'taxiData/HVFHV/2023/fhvhv_tripdata_2023-07.parquet',
    hdfs_directory_path + 'taxiData/HVFHV/2023/fhvhv_tripdata_2023-08.parquet',
    hdfs_directory_path + 'taxiData/HVFHV/2023/fhvhv_tripdata_2023-09.parquet',
    hdfs_directory_path + 'taxiData/HVFHV/2023/fhvhv_tripdata_2023-10.parquet'
]


df = spark.read.parquet(*file_paths)
df2 = df.select(columnsToKeep)
dfWithFileName = df2.withColumn('hvfhv-2019', input_file_name())
#for Green Data: df3 = df2.withColumn('hour_range', expr("concat(date_format(lpep_pickup_datetime, 'HH:00:00'), ' - ', date_format(lpep_pickup_datetime, 'HH:59:59'))"))
df3 = df2.withColumn('timestamp', expr("concat(date_format(request_datetime, 'HH:00:00'), ' - ', date_format(request_datetime, 'HH:59:59'))"))
df4 = df3.groupBy('timestamp').count().sort(col('timestamp'))
df4.write.csv('/user/s3105555/project', header=True, mode='overwrite')