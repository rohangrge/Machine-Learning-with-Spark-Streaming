from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SQLContext
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType
from pyspark.ml.feature import HashingTF, IDF, RegexTokenizer
from pyspark.sql.functions import array


def readMyStream(rdd):

    if not rdd.isEmpty():
        global batch_no
        batch_no += 1
        # convert the json object into a dataframe
        df = spark.read.json(rdd)
        df_final = spark.createDataFrame(data=[], schema=schema)

        '''Each row in a batch is read as a seperate column, we need to extract the features
(Subject, Message, Spam/Ham) provided by each row and present them in the required dataframe format'''

        for i in df.columns:
            df_temp = df.select("{}.feature0".format(
                i), "{}.feature1".format(i), "{}.feature2".format(i))
            df_final = df_final.union(df_temp)
            print(i)
        df_final.show()
        df_final = df_final.withColumn(
            "feature1a", lower_clean_str(F.col("feature1")))
        df_final = df_final.withColumn("feature1", array(df_final.feature1a))
        tokenizer = RegexTokenizer(inputCol="feature1", outputCol="words")
        wordsData = tokenizer.transform(df_final)
        hashingTF = HashingTF(inputCol="feature1",
                              outputCol="rawFeatures", numFeatures=20)
        featurizedData = hashingTF.transform(wordsData)
        idf = IDF(inputCol="feature1", outputCol="features")
        idfModel = idf.fit(featurizedData)
        rescaledData = idfModel.transform(featurizedData)
        for features_label in rescaledData.select("features", "feature0").take(3):
            print(features_label)
        print(batch_no)
        df_final.show()


def lower_clean_str(x):
    punc = '!"#$%&\'()*+,-./:;<=>?@[\\]^_`{|}~'
    lowercased_str = x.lower()
    for ch in punc:
        lowercased_str = lowercased_str.replace(ch, '')
    return lowercased_str


# schema for the final dataframe
schema = StructType().add("feature0", StringType()) \
    .add("feature1", StringType()).add("feature2", StringType())

# initialisations
sc = SparkContext("local[2]", "spam")
spark = SparkSession(sc)
ssc = StreamingContext(sc, 1)
batch_no = 0


# read streaming data from socket into a dstream
lines = ssc.socketTextStream("localhost", 6100)
# process each RDD(resilient distributed dataset) to desirable format
lines.foreachRDD(lambda rdd: readMyStream(rdd))

ssc.start()
ssc.awaitTermination()
