A = sc.textFile("/Project/A.csv", minPartitions=10)
B = sc.textFile("/Project/B.csv", minPartitions=10)

def left(line):
    """Function to read array as row array.
    
    Parameters
    ----------
    
    line: string
        CSV delimited string
    
    Returns
    -------
    
    A: tuple of int, list
        key   -> number of row
        value -> list of column, value (1 element)
    """
    contents = line.split(",")
    row, col, val = map(int, contents)
    return (row, [(col, val)])

def right(line):
    """Function to read array as column array.
    
    Parameters
    ----------
    
    line: string
        CSV delimited string.
    
    Returns
    -------
    
    A: tuple of int, list
        key   -> number of column
        value -> list of row, value (1 element)
    """
    contents = line.split(",")
    row, col, val = map(int, contents)
    return (col, [(row, val)])

def concat(a, b):
    """Concat two lists.
    
    Parameters
    ----------
    
    a: list
    
    b: list
    
    Returns
    -------
    
    a + b: list
    """
    return a + b

def sort_remove(value):
    """Function to sort by first value and then remove it.
    
    Useful so that the values of each row/column are in the correct order.
    
    Parameters
    ----------
    
    value: list of tuples
        A row or a column of an array.
        
    Returns
    -------
    
    value: list of number
        Correctly ordered row/column.
    """
    value.sort(key=lambda x: x[0])
    return list(map(lambda x: x[1], value))

L = A.map(left)\
     .reduceByKey(concat)\
     .mapValues(sort_remove)

R = B.map(right)\
     .reduceByKey(concat)\
     .mapValues(sort_remove)

C = L.cartesian(R)\
     .map(lambda x: ((x[0][0], x[1][0]), (x[0][1], x[1][1])))

def mul_and_add(row, col):
    """Function to perform multiplication and adding of two lists by element.
    
    Parameters
    ----------
    
    row: list of numbers

    col: list of numbers
    
    Returns
    -------
    
    sum(row * col): number
    """
    return sum(map(lambda x: x[0] * x[1], zip(row, col)))

C = C.mapValues(lambda x: mul_and_add(x[0], x[1]))\
     .map(lambda x: (x[0][0], x[0][1], x[1]))

df = C.toDF(["row", "column", "value"])\
      .sort(["row", "column"])

df.coalesce(1)\
  .write\
  .csv("/Project/Matrix", header="true", mode="overwrite")
