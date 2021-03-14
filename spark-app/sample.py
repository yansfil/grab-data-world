import sys
from pyspark import SparkContext

sc = SparkContext()

logFile = sys.argv[1]

logData = sc.textFile(logFile).cache()

numAs = logData.filter(lambda s: 'a' in s).count()

numBs = logData.filter(lambda s: 'b' in s).count()

print("Lines with a: {}, lines with b: {}".format(numAs, numBs))
