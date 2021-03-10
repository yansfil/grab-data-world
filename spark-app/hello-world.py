import sys
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession

##########################
# You can configure master here if you do not pass the spark-app.master paramenter in conf
##########################
#master = "spark-app://spark-app:7077"
#conf = SparkConf().setAppName("Spark Hello World").setMaster(master)
#sc = SparkContext(conf=conf)
#spark-app = SparkSession.builder.config(conf=conf).getOrCreate()

# Create spark-app context
sc = SparkContext()

# Get the second argument passed to spark-app-submit (the first is the python app)
logFile = sys.argv[1]

# Read file
logData = sc.textFile(logFile).cache()

# Get lines with A
numAs = logData.filter(lambda s: 'a' in s).count()

# Get lines with B 
numBs = logData.filter(lambda s: 'b' in s).count()

# Print result
print("Lines with a: {}, lines with b: {}".format(numAs, numBs))
