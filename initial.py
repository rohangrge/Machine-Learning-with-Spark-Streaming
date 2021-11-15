from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SQLContext
import pyspark.sql.functions as f
from pyspark.sql import SparkSession

spark_context = SparkContext("local[2]", "detective")
spark = SparkSession.builder.master("local").appName("spam").getOrCreate()
ssc = StreamingContext(spark_context,1)

lines = ssc.socketTextStream('localhost',6100)
df = spark.createDataFrame([lines])

df.printSchema()
print(df.dtypes)

ssc.start()
ssc.awaitTermination()
