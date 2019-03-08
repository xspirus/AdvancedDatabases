from functools import partial

data = sc.textFile("/Project/yellow_tripdata_1m.csv", minPartitions=50)

def euclidean(p, q):
    """Calculate euclidean distance.
    
    Formula:
    .. math::
        \sqrt{\sum (p_i - q_i)^2}
        
    Parameters
    ----------
    
    p: iterable of numbers
        Contains the coordinates of the first point.
        
    q: iterable of numbers
        Contains the coordinates of the second point.
        
    Returns
    -------
    
    euclidean: float
    """
    sum = 0
    for pi, qi in zip(p, q):
        sum += (pi - qi) ** 2
    return sum ** (1 / 2)

def coordinates(line):
    """Function to map over RDD lines.
    
    Produces the necessary Coordinates tuples.
    
    Parameters
    ----------
    
    line: string
        CSV delimited string from RDD.
        
    Returns
    -------
    
    coordinates: tuple of numbers
    """
    contents = line.split(",")
    lng, lat = map(float, contents[3:5])
    return lng, lat

population = data.map(coordinates)
k = 5
iterations = 3
centroids = [(i, c) for i, c in enumerate(population.take(k), 1)]

def closest(centroids, coordinates):
    """Find closest centroid.
    
    Computes the minimum euclidean distance
    and saves centroid id.
    
    Parameters
    ----------
    
    centroids: iterable
        Contains the centroids as tuples of id, coordinates.
    
    coords: tuple of floats
        Tuple contains the coordinates.
        
    Returns
    -------
    
    id_coords: tuple of int, tuple
        Tuple containing the id of the closest centroid and
        the tuple of coordinates of this point.
    """
    distance = min(
        (
            (centroid[0], euclidean(coordinates, centroid[1])) 
            for centroid in centroids
        ),
        key=lambda x: x[1]
    )
    return (distance[0], coordinates)

def sum_by_elem(p, q):
    """Function to reduce over RDD values.
    
    Gets two elements and sums each coordinate.
    
    Parameters
    ----------
    
    p: tuple of tuple, int
        Contains tuple of floats and an int
    
    q: tuple of tuple, int
        Contains tuple of floats and an int
    
    Returns
    -------
    
    sum_by_elem: tuple of tuple, int
        Tuple of floats, int
    """
    p, num1 = p
    q, num2 = q
    return (tuple(map(sum, zip(p, q))), num1 + num2)

def avg_by_elem(p):
    """Function to reduce over RDD values.
    
    Gets an element and averages each coordinate.
    
    Parameters
    ----------
    
    p: tuple of tuple, int
        Contains a tuple with summed by element values, sum of ints
    
    Returns
    -------
    
    avg_by_elem: tuple of floats
        For each tuple element divide with number of elements
    """
    p, num = p
    return tuple(map(lambda x: x / num, p))

for _ in range(iterations):
    pclosest = partial(closest, centroids)
    points_labels = population.map(pclosest)
    new_centroids = points_labels.mapValues(lambda x: (x, 1))\
                                 .reduceByKey(sum_by_elem)\
                                 .mapValues(avg_by_elem)
    centroids = new_centroids.collect()

centroids = new_centroids.toDF(["Id", "Centroid"])
centroids.withColumn("Centroid", centroids.Centroid.cast("string"))\
         .coalesce(1)\
         .write\
         .csv(
              "/Project/Centroids",
              header="true",
              mode="overwrite",
              quote=""
          )
