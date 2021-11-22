from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SQLContext
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType


def readMyStream(rdd):
  if not rdd.isEmpty():
    df = spark.read.json(rdd)
    print('Started the Process')
    print('Selection of Columns')
    df_final = spark.createDataFrame(data = [] , schema = schema)
    #df = df.select("feature0", "feature1", "feature2")
    #print(df.columns)
    for i in df.columns:
       #print(i)
       df_temp = df.select("{}.feature0".format(i), "{}.feature1".format(i), "{}.feature2".format(i))
       #df_temp.show()
       df_final = df_final.union(df_temp)
    
    #Assuming that all the json columns are in a single column, hence making it an array column first.
    # df = df.withColumn("array_col", F.array(df.columns))
    # #Then explode and getItem
    # df = df.withColumn("explod_col", F.explode("array_col"))
    # df = df.select("*", F.explode("explod_col").alias("x", "y"))
    # df_final = df.withColumn("seq", df.y.getItem("seq")).withColumn("state", df.y.getItem("state")).withColumn("CMD", df.y.getItem("CMD"))
    # df_final.select("seq","state","CMD").show()
    df_final.show()
     
    #df.printSchema()
    #print(df.columns)

#inner_cols = ["feature0", "feature1", "feature2"]

schema = StructType().add("feature0", StringType()) \
.add("feature1", StringType()).add("feature2", StringType())

# cols = [[str(x)[inner_cols[0]], str(x)[inner_cols[1]], str(x)[innercols[2]] ] for x in range(0,12)]
# print(cols)

sc = SparkContext("local[2]", "spam")
spark = SparkSession(sc)
ssc = StreamingContext(sc, 1)

lines = ssc.socketTextStream("localhost", 6100)

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