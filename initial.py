from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SQLContext
import pyspark.sql.functions as f
from pyspark.sql import SparkSession

spark_context = SparkContext("local[5]", "detective")
spark = SparkSession.builder.master("local").appName("spam").getOrCreate()
ssc = StreamingContext(spark_context,1)
sqlContext = SQLContext(spark_context)

lines = ssc.socketTextStream("localhost", 6100)
linesRDD = spark_context.parallelize([lines])
df = spark.read.json([linesRDD])

df.printSchema()
print(df.dtypes)
#print(lines)
ssc.start()
ssc.awaitTermination()
