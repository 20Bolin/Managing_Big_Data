"""
The following script demonstrates how to automate the download of data using URI from internet to HDFS.(Here the green taxi data of 2019 is used as an example.)
"""

'''
I wrote this script to help automate the process of downloading data from the Internet into HDFS
and to avoid storing too much data in NFS simutaneously.
Execute this program in spark environment, or run it in our own IDE but you should have internet access to the UT spark framework and the path should also be changed accordingly.
'''

import os
import wget
import subprocess

#Put the urls from which you pull data into NFS system in this list
urls = ["https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2019-01.parquet",
        "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2019-02.parquet",
        "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2019-03.parquet",
        "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2019-04.parquet",
        "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2019-05.parquet",
        "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2019-06.parquet",
        "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2019-07.parquet",
        "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2019-08.parquet",
        "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2019-09.parquet",
        "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2019-10.parquet",
        "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2019-11.parquet",
        "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2019-12.parquet"
        ]

my_nfs_path = "/home/s3022455"
my_hdfs_path = "/user/s3022455/taxiData/Green/2019"

for url in urls:
    #Use wget to download data into my NFS system. Here I use my own path. If you wanna customizs it you should set it above as your own path.
    filename = wget.download(url, out=my_nfs_path)

    #Transfer the data from my NFS to my HDFS
    subprocess.check_call(['hdfs', 'dfs', '-put', filename, my_hdfs_path])

    #Delete the data in NFS
    os.remove(filename)