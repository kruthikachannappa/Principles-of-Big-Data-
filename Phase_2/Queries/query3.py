from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
import matplotlib as plt
import matplotlib.pyplot as plt
import pandas as pd

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .getOrCreate()
# spark is an existing SparkSession
df = spark.read.json("E:\PB Elections data\extractTweetsM.json")
# Displays the content of the DataFrame to stdout

# Register the DataFrame as a SQL temporary view
df.createOrReplaceTempView("Elections")
sqlDF = spark.sql("SELECT 'BJP' as party, count(*) as votes from Elections where text like '%BJP%' and text like '%support%'\
        UNION\
        SELECT 'congress' as party, count(*) as votes from Elections where text like '%congress%' and text like '%support%'")
pd = sqlDF.toPandas()
pd.to_csv('fourth.csv', index=False)
pd.plot(kind="bar", x="party", y="votes")
plt.show()
sqlDF.show()