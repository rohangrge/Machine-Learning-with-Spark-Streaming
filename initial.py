from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SQLContext
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType
from pyspark.ml.feature import HashingTF, IDF, RegexTokenizer
from pyspark.sql.functions import array, lower, regexp_replace, trim, col
from pyspark.ml.linalg import VectorUDT
from pyspark.ml.feature import StopWordsRemover
import nltk
from nltk.corpus import stopwords
from sparknlp.annotator import (Tokenizer, Normalizer,
                                LemmatizerModel, StopWordsCleaner)
nltk.download('stopwords')


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
            "feature1", removePunctuation(col("feature1")))
        # df_final = df_final.withColumn("feature1", array(df_final.feature1a))
        # df_final.show()
        tokenizer = RegexTokenizer(inputCol="feature1", outputCol="words")
        wordsData = tokenizer.transform(df_final)
        wordsData = wordsData.withColumn("feature1", array(df_final.feature1))
        lemmatizer = LemmatizerModel.pretrained().setInputCols(
            ['words']).setOutputCol('lemma')
        lemmatizedData = lemmatizer.transform(wordsData)
        remover = StopWordsRemover(
            inputCol="lemma", outputCol="filtered", stopWords=stopwords.words("english"))
        stopRemoval = remover.transform(lemmatizedData)
        stopRemoval.show()

        hashingTF = HashingTF(inputCol="feature1",
                              outputCol="rawFeatures", numFeatures=1)
        featurizedData = hashingTF.transform(stopRemoval)

        idf = IDF(inputCol="rawFeatures", outputCol="features")
        idfModel = idf.fit(featurizedData)
        rescaledData = idfModel.transform(featurizedData)
        for features_label in rescaledData.select("features", "feature0").take(3):
            print(features_label)
        print(batch_no)
        df_final.show()


def removePunctuation(column):
    """Removes punctuation, changes to lower case, and strips leading and trailing spaces.

    Note:
        Only spaces, letters, and numbers should be retained.  

    Args:
        column (Column): A Column containing a sentence.

    Returns:
        Column: A Column named 'sentence' with clean-up operations applied.
    """
    return lower(trim(regexp_replace(column, '\\p{Punct}', ''))).alias('sentence')


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
