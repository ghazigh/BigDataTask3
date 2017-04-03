
# coding: utf-8

# In[4]:

import os
import sys


# In[5]:

spark_path = "E:/spark"


# In[6]:

os.environ['SPARK_HOME'] = spark_path


# In[7]:

os.environ['HADOOP_HOME'] = spark_path


# In[8]:

os.environ['PYSPARK_PYTHON'] = sys.executable


# In[9]:

sys.path.append(spark_path + "/bin")
sys.path.append(spark_path + "/python")
sys.path.append(spark_path + "/python/pyspark/")
sys.path.append(spark_path + "/python/lib")
sys.path.append(spark_path + "/python/lib/pyspark.zip")
sys.path.append(spark_path + "/python/lib/py4j-0.10.4-src.zip")


# In[10]:

from pyspark import SparkContext
from pyspark import SparkConf

sc = SparkContext("local", "test")

print sc


# In[11]:

sc


# In[16]:

from pyspark.mllib.fpm import FPGrowth
from pyspark.mllib.fpm import FPGrowthModel
from pyspark.mllib.fpm import PrefixSpan


# In[13]:

data = [
            [["a", "b"], ["c"]],
            [["a"], ["c", "b"], ["a", "b"]],
            [["a", "b"], ["e"]],
            [["f"]]]


# In[14]:

rdd = sc.parallelize(data, 2)


# In[17]:

model = PrefixSpan.train(rdd)


# In[18]:

sorted(model.freqSequences().collect())


# In[ ]:



