import numpy as np
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SQLContext
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, IntegerType
from pyspark.ml.feature import HashingTF, IDF, RegexTokenizer, Binarizer
from pyspark.sql.functions import array, lower, regexp_replace, trim, col
from pyspark.ml.linalg import VectorUDT
from pyspark.ml.feature import StopWordsRemover
import nltk
from nltk.corpus import stopwords
from sparknlp.base import Finisher, DocumentAssembler
from pyspark.ml import Pipeline
from sparknlp.annotator import (Tokenizer, Normalizer,
                                LemmatizerModel, StopWordsCleaner)
from sklearn.naive_bayes import GaussianNB
import pickle

from pyspark.ml.feature import StringIndexer
nltk.download('stopwords')
udf_spam_encode = F.udf(lambda x: spam_encoding(x), IntegerType())


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

        df_final = df_final.withColumn(
            "feature2a", udf_spam_encode(col("feature2")))
        df_final.show()

        df_final = df_final.withColumn(
            "feature1", removePunctuation(col("feature1")))

        equifax = pipeline.fit(df_final).transform(df_final)
        # print(equifax.columns)
        # for features_label in equifax.select("finished_clean_lemma").take(1):
        #     print(features_label)

        hashingTF = HashingTF(inputCol="finished_clean_lemma",
                              outputCol="rawFeatures", numFeatures=1)
        featurizedData = hashingTF.transform(equifax)

        idf = IDF(inputCol="rawFeatures", outputCol="features")
        idfModel = idf.fit(featurizedData)
        rescaledData = idfModel.transform(featurizedData)
        gnb.partial_fit((rescaledData.select("features").collect())[
                        0], rescaledData.select("feature2a").collect()[0], classes=[0, 1])

        # gnb.predict(rescaledData.select("features"))
        # rescaledData.show()

        # for features_label in rescaledData.select("features", "feature0").take(3):
        #     print(features_label)
        print(batch_no)
        # df_final.show()


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


def spam_encoding(spam_ham):
    if spam_ham == "spam":
        return 0
    else:
        return 1


eng_stopwords = stopwords.words('english')

# schema for the final dataframe
schema = StructType().add("feature0", StringType()) \
    .add("feature1", StringType()).add("feature2", StringType())

# initialisations
sc = SparkContext("local[2]", "spam")
spark = SparkSession(sc)
ssc = StreamingContext(sc, 1)
batch_no = 0

documentAssembler = DocumentAssembler() \
    .setInputCol('feature1') \
    .setOutputCol('document')

tokenizer = Tokenizer() \
    .setInputCols(['document']) \
    .setOutputCol('token')

# note normalizer defaults to changing all words to lowercase.
# Use .setLowercase(False) to maintain input case.
normalizer = Normalizer() \
    .setInputCols(['token']) \
    .setOutputCol('normalized') \
    .setLowercase(True)

# note that lemmatizer needs a dictionary. So I used the pre-trained
# model (note that it defaults to english)
lemmatizer = LemmatizerModel.pretrained() \
    .setInputCols(['normalized']) \
    .setOutputCol('lemma') \

stopwords_cleaner = StopWordsCleaner() \
    .setInputCols(['lemma']) \
    .setOutputCol('clean_lemma') \
    .setCaseSensitive(False) \
    .setStopWords(eng_stopwords)

# finisher converts tokens to human-readable output
finisher = Finisher() \
    .setInputCols(['clean_lemma']) \
    .setCleanAnnotations(True)

pipeline = Pipeline() \
    .setStages([
        documentAssembler,
        tokenizer,
        normalizer,
        lemmatizer,
        stopwords_cleaner,
        finisher
    ])
gnb = GaussianNB()
# read streaming data from socket into a dstream
lines = ssc.socketTextStream("localhost", 6100)
# process each RDD(resilient distributed dataset) to desirable format
lines.foreachRDD(lambda rdd: readMyStream(rdd))

ssc.start()


ssc.awaitTermination()
with open('my_dumped_classifier.pkl', 'wb') as fid:
    pickle.dump(gnb, fid)
