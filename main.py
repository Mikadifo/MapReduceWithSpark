from pyspark.sql import SparkSession
from utils import mergeTuple, mergeValue

spark = SparkSession.builder.appName("project2").getOrCreate()

nodesFile = "data/nodes.tsv"
edgesFile = "data/edges.tsv"

nodesRdd = spark.sparkContext.textFile(nodesFile).map(lambda line: line.split("\t"))
edgesRdd = spark.sparkContext.textFile(edgesFile).map(lambda line: line.split("\t"))

def queryOne():
    result = edgesRdd.filter(lambda cols: cols[1] in ["CtD", "CpD", "CbG"])
    result = result.map(lambda cols:
                        (cols[0], (
                            1 if cols[2].__contains__("Gene") else 0,
                            1 if cols[2].__contains__("Disease") else 0)))
    result = result.reduceByKey(mergeTuple)
    result = result.map(lambda pair: (pair[0], pair[1][0], pair[1][1]))
    result = result.takeOrdered(5, key=lambda t: -t[1])

    spark.createDataFrame(result, ["Drug", "#genes", "#diseases"]).show()

def queryTwo():
    result = edgesRdd.filter(lambda cols: cols[1] in ["CtD", "CpD"])
    result = result.map(lambda cols: (cols[0], 1))
    result = result.reduceByKey(mergeValue)
    result = result.groupBy(lambda pair: pair[1])
    result = result.mapValues(lambda t: len(t))
    result = result.map(lambda pair: (pair[1], pair[0]))
    result = result.reduceByKey(mergeValue)
    result = result.takeOrdered(5, key=lambda t: -t[1])

    spark.createDataFrame(result, ["n drugs", "#diseases"]).show()

def queryThree():
    result = edgesRdd.filter(lambda cols: cols[1] == "CbG")
    result = result.map(lambda cols: (cols[0], 1))
    result = result.reduceByKey(mergeValue)
    result = result.join(nodesRdd.map(lambda line: (line[0], line[1])))
    result = result.map(lambda pair: (pair[1][0], pair[1][1]))
    result = result.takeOrdered(5, key=lambda t: -t[0])

    spark.createDataFrame(result, ["#genes", "Drug name"]).show()

selection = 0
while selection != 4:
    print("\nWelcome!\n")
    print("1. Query One\n2. Query Two\n3. Query Three\n4. Exit\n")

    while selection < 1 or selection > 4:
        selection = int(input("Select a query: "))

    if selection == 1:
        selection = 0
        queryOne()

    if selection == 2:
        selection = 0
        queryTwo()

    if selection == 3:
        selection = 0
        queryThree()

spark.stop()
