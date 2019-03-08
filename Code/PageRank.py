from functools import partial
from operator import add

data = sc.textFile("/Project/web-Google.txt", minPartitions=10)

def parse_nodes(line):
    """Function to map over RDD lines.
    
    Finds the from and to ids.
    
    Parameters
    ----------
    
    line: string
        CSV (space) delimited line from RDD.
        
    Returns
    -------
    
    nodes: tuple of ints
    """
    contents = line.split()
    return tuple(map(int, contents))

web = data.map(parse_nodes)
adjacent = web.groupByKey()\
              .mapValues(list)
score = adjacent.mapValues(lambda _: 0.5)
iterations = 5
N = 875713
d = 0.85

def contribution(nodes, score):
    """Generator function for flatMap.
    
    Finds for each outgoing node the:
    .. math::
        \frac{PR(p_j)}{L(p_j)}
        
    Parameters
    ----------
    
    nodes: iterable
        Contains the node ids.
        
    score: float
        The PR of the current node.
        
    Yields
    ------
    
    node_score: tuple of int, float
        The tuple containing the node id and its new score
        from this outgoing node.
    """
    L = len(nodes)
    for node in nodes:
        yield (node, score / L)

def PR(N, d, score):
    return ((1 - d) / N) + (d * score)

PPR = partial(PR, N, d)

for _ in range(iterations):
    score = adjacent.join(score, numPartitions=10)\
                    .flatMap(
                         lambda x: contribution(x[1][0], x[1][1])
                     )\
                    .reduceByKey(add)\
                    .mapValues(PPR)

score = score.toDF(["NodeId", "PageRank"])\
             .sort("NodeId")
score.coalesce(1)\
     .write\
     .csv("/Project/PageRank", header="true", mode="overwrite")
