import datetime as dt

vendors = sc.textFile("/Project/yellow_tripvendors_1m.csv", minPartitions=50)
data = sc.textFile("/Project/yellow_tripdata_1m.csv", minPartitions=50)

def start_duration(line):
    """Function to map over RDD lines.
    
    Produces the necessary tuples for key - value processing.
    The starttime of the trip becomes the key, while the duration
    of the trip becomes the value.
    
    Parameters
    ----------
    
    line: string
        CSV delimited string from RDD.
        
    Returns
    -------
    
    keyval: tuple of (string, float)
        Key   -> starttime
        Value -> duration
    """
    contents = line.split(",")
    start, end = contents[1], contents[2]
    starttime = dt.datetime.strptime(start, "%Y-%m-%d %H:%M:%S")
    endtime = dt.datetime.strptime(end, "%Y-%m-%d %H:%M:%S")
    diff = endtime - starttime
    diff = diff.total_seconds() / 60
    return "{:02d}".format(starttime.hour), diff

duration = data.map(start_duration)

duration = duration.mapValues(lambda x: (x, 1))\
                   .reduceByKey(
                        lambda a, b: tuple(map(sum, zip(a, b)))
                    )\
                   .mapValues(lambda x: x[0] / x[1])

df = duration.toDF(["HourOfDay", "AverageTripDuration"])\
             .sort("HourOfDay")
df.coalesce(1)\
  .write\
  .csv("/Project/TripDuration", header="true", mode="overwrite")
