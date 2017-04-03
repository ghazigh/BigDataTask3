
# coding: utf-8

# In[2]:

import os
import sys


# In[3]:

spark_path = "E:/spark"


# In[4]:

os.environ['SPARK_HOME'] = spark_path


# In[5]:

os.environ['HADOOP_HOME'] = spark_path


# In[6]:

os.environ['PYSPARK_PYTHON'] = sys.executable


# In[7]:

sys.path.append(spark_path + "/bin")
sys.path.append(spark_path + "/python")
sys.path.append(spark_path + "/python/pyspark/")
sys.path.append(spark_path + "/python/lib")
sys.path.append(spark_path + "/python/lib/pyspark.zip")
sys.path.append(spark_path + "/python/lib/py4j-0.10.4-src.zip")


# In[8]:

from pyspark import SparkContext
from pyspark import SparkConf

sc = SparkContext("local", "test")

print sc


# In[9]:

sc


# In[14]:

from pyspark.mllib.fpm import FPGrowth


# from pyspark.mllib.fpm import AssociationRules

# In[15]:

if __name__ == "__main__":
    data = sc.textFile("file:///E:/BIG DATA/FPGrowth/sample_fpgrowth.txt")
    transactions = data.map(lambda line: line.strip().split(' '))
    model = FPGrowth.train(transactions, minSupport=0.2, numPartitions=10)
    result = model.freqItemsets().collect()
    for fi in result:
        print(fi)


# In[17]:

from pyspark.mllib.fpm import PrefixSpan


# In[ ]:

rdd

