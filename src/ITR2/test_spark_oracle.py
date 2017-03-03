from pyspark import SparkContext
sc = SparkContext()
textFile = sc.textFile("Python_Kafka.py")
print(textFile.count())
