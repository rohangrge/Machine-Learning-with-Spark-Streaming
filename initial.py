from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SQLContext
import pyspark.sql.functions as f
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType


def readMyStream(rdd):
  if not rdd.isEmpty():
    df = spark.read.json(rdd)
    print('Started the Process')
    print('Selection of Columns')
    df = df.select(cols)
    df.show()

cols = [ str(x) for x in range(0,12)]
sc = SparkContext("local[2]", "spam")
spark = SparkSession(sc)
ssc = StreamingContext(sc, 1)

lines = ssc.socketTextStream("localhost", 6100)
# .map(lambda x : check_json(x,cols)) \
#       .filter(lambda x : x) \
#             .foreachRDD(lambda x : convert_json2df(x,cols))

lines.foreachRDD(lambda rdd : readMyStream(rdd))



    

#lines.pprint()
#df.printSchema()
#print(df.dtypes)
#print(lines)
ssc.start()
ssc.awaitTermination()

















# from pyspark.sql import SparkSession
# import pyspark.sql.types as tp
# from pyspark import SparkContext
# from pyspark.sql import SQLContext
# from pyspark.streaming import StreamingContext
# from pyspark.sql.functions import from_json
# from pyspark.sql.types import StructType, StringType
# from sklearn.feature_extraction import text
# from pyspark.ml.feature import StopWordsRemover
# #import spark.implicits._

# spark_context = SparkContext.getOrCreate()
# spark = SparkSession.builder.master("local").appName("spam").getOrCreate()
# sqlContext = SQLContext(spark_context)
# #stop = text.ENGLISH_STOP_WORDS

# #schema = StructType().add("Subject", StringType()) \
# #.add("Message", StringType()).add("Spam/Ham", StringType())


# lines = spark.readStream.format("socket") \
# .option("host","localhost").option("port",6100).load()

# # print(lines.isStreaming())
# # print(lines.schema)

# print(lines.isStreaming)
# lines.printSchema
# words = lines.select("*")
# #Generate running word count
# # wordCounts = words.groupBy("word").count()

# query = words.writeStream.format("console").outputMode("append").start().awaitTermination()

# #remover = StopWordsRemover(stopWords = stop)



# # query = lines.writeStream.format("console").outputMode("append").start().awaitTermination()