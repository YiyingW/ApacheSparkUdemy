'''
run it on the Spark cluster by
spark-submit logs.py
'''

from pyspark import SparkConf, SparkContext

# the SparkConf helps us specify the parameters of the Spark Connection
conf = SparkConf().setMaster("local[5]").setAppName('test') # 5 threads
# setMaster can take parameter like "yarn-client"
sc = SparkContext(conf=conf)

# some code here
logsPath = '../data/hbase.log'
logs = sc.textFile(logsPath)
errCount = sc.accumulator(0)

def processLog(line):
	global errCount
	dateField = line[:24]
	logField = line[24:]

	if "ERROR" in line:
		errCount+=1
	return (dateField, logField)

logs.map(processLog).saveAsTextFile('../data/log')

print "There were " + str(errCount.value) + " ERROR lines."