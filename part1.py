#!/usr/bin/env python3

from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext

import datetime as dt

conf = SparkConf().setAppName("testing").setMaster("local[2]")
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

# ------------------------------ Test reading ------------------------------ #

vendors = sc.textFile("hdfs:///Project/yellow_tripvendors_1m.csv")
data = sc.textFile("hdfs:///Project/yellow_tripdata_1m.csv")

# --------------------------------- Part 1a -------------------------------- #


def calculate_key_value_1(line):
    contents = line.split(",")
    start, end = contents[1], contents[2]
    starttime = dt.datetime.strptime(start, "%Y-%m-%d %H:%M:%S")
    endtime = dt.datetime.strptime(end, "%Y-%m-%d %H:%M:%S")
    diff = endtime - starttime
    diff = diff.total_seconds() / 60
    return starttime.hour, diff


ndata = data.map(calculate_key_value_1)
ndata.repartition(50)

ndata = (
    ndata.mapValues(lambda x: (x, 1))
    .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))
    .mapValues(lambda x: x[0] / x[1])
)

df = ndata.toDF(["HourOfDay", "AverageTripDuration"])
df.coalesce(1).write.csv("/Project/TripDuration", mode="overwrite", header="true")

ndata.unpersist()
df.unpersist()

# --------------------------------- Part 1b ---------------------------------- #


def calculate_key_value_2(line):
    contents = line.split(",")
    return contents[0], contents[7]


tvendors = vendors.map(lambda x: tuple(x.split(",")))
tdata = data.map(calculate_key_value_2)

tvendors = sqlContext.createDataFrame(tvendors, ["id", "company"])
tdata = sqlContext.createDataFrame(tdata, ["id", "price"])

alldata = tvendors.join(tdata, on="id", how="inner")

tvendors.unpersist()
tdata.unpersist()

alldata = alldata.rdd
alldata.repartition(50)

alldata = alldata.map(lambda x: (x["company"], float(x["price"]))).reduceByKey(
    max
)

df = alldata.toDF(["VendorID", "MaxAmount"])
df.coalesce(1).write.csv("/Project/MaxAmount", mode="overwrite", header="true")

alldata.unpersist()
df.unpersist()
