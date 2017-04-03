
# coding: utf-8

# In[1]:

import os
import sys


# In[2]:

spark_path = "E:/spark"


# In[3]:

os.environ['SPARK_HOME'] = spark_path


# In[4]:

os.environ['HADOOP_HOME'] = spark_path


# In[5]:

os.environ['PYSPARK_PYTHON'] = sys.executable


# In[6]:

sys.path.append(spark_path + "/bin")
sys.path.append(spark_path + "/python")
sys.path.append(spark_path + "/python/pyspark/")
sys.path.append(spark_path + "/python/lib")
sys.path.append(spark_path + "/python/lib/pyspark.zip")
sys.path.append(spark_path + "/python/lib/py4j-0.10.4-src.zip")


# In[7]:

from pyspark import SparkContext
from pyspark import SparkConf

sc = SparkContext("local", "test")

print sc


# In[8]:

sc


# In[9]:

from pyspark.mllib.fpm import FPGrowth


# In[10]:

data = [["a", "b", "c"], ["a", "b", "d", "e"], ["a", "c", "e"], ["a", "c", "f"]]


# In[11]:

rdd = sc.parallelize(data, 2)


# In[12]:

model = FPGrowth.train(rdd, 0.6, 2)


# In[13]:

sorted(model.freqItemsets().collect())


# In[30]:

model_path = "BIG DATA/FPGrowth/"


# In[31]:

print model_path


# In[32]:

model.save(sc, model_path)


# In[33]:

from pyspark.mllib.fpm import FPGrowthModel


# In[34]:

sameModel = FPGrowthModel.load(sc, model_path)


# In[35]:

sorted(model.freqItemsets().collect()) == sorted(sameModel.freqItemsets().collect())


# In[ ]:



