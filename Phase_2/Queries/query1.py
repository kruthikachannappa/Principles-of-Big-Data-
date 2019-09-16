from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
import matplotlib.pyplot as plt
import pandas as pd

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .getOrCreate()
# spark is an existing SparkSession
df = spark.read.json("E:\PB Elections data\extractTweetsM.json")
# Displays the content of the DataFrame to stdout
# df.show()
# Register the DataFrame as a SQL temporary view
df.createOrReplaceTempView("Politics")
sqlDF = spark.sql("SELECT COUNT(*) AS NumberOfTweets, 'PawanKalyan' as Language FROM Politics where text LIKE '%pawankalyan%'\
        UNION\
        SELECT COUNT(*) AS NumberOfTweets,'ChandraBabu' as Language FROM Politics where text LIKE '%chandrababu%'\
        UNION\
        SELECT COUNT(*) AS NumberOfTweets, 'Jagan Mohan' as Language FROM Politics where text LIKE '%jagan%'\
        UNION\
        SELECT COUNT(*) AS NumberOfTweets, 'KCR' as Language FROM Politics where text LIKE '%kcr%'\
         ")

pd = sqlDF.toPandas()
pd.to_csv('first.csv', index=False)
pd.plot.pie(y='NumberOfTweets', labels=['Pawankalyan','ChandraBabu', 'Jagan Mohan', 'KCR'], figsize=(5, 5))
plt.show()
sqlDF.show()