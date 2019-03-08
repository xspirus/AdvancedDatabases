vendors = sc.textFile("/Project/yellow_tripvendors_1m.csv", minPartitions=50)
data = sc.textFile("/Project/yellow_tripdata_1m.csv", minPartitions=50)

def id_price(line):
    """Function to map over RDD lines.
    
    Produces the necessary tuples for key - value processing.
    The id of the trip becomes the key, while the price of
    the trip becomes the value.
    
    Parameters
    ----------
    
    line: string
        CSV delimited string from RDD.
        
    Returns
    -------
    
    idprice: tuple of (string, float)
        Key   -> id of trip
        Value -> price of trip
    """
    contents = line.split(",")
    return contents[0], float(contents[7])

vendors = vendors.map(lambda x: tuple(x.split(",")))
prices = data.map(id_price)

prices = vendors.join(prices)

prices = prices.map(lambda x: (x[1][0], x[1][1]))\
               .reduceByKey(max)

df = prices.toDF(["VendorID", "MaxAmountPaid"])\
           .sort("VendorID")

df.coalesce(1)\
  .write\
  .csv("/Project/MaxAmountPaid", header="true", mode="overwrite")
