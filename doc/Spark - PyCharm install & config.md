## Installation
click [__here__](https://www.jetbrains.com/pycharm/download/) to download PyCharm, choose ` community version ` will be ok.

## Configuration

Way 1:

```python
# -*- coding: UTF-8 -*-
# author: 

import os
import sys
os.environ['SPARK_HOME']="/Users/similarface/spark-1.6.0-bin-hadoop2.6"
sys.path.append("/Users/similarface/spark-1.6.0-bin-hadoop2.6/python")
 
try:
    from pyspark import SparkContext,SparkConf
    print("Successfully imported Spark Modules")
except ImportError as e:
    print("Can not import Spark Modules", e)
    sys.exit(1)
```

result: ` Successfully imported Spark Modules `

Way 2:

![pic1](http://images2015.cnblogs.com/blog/879525/201604/879525-20160425170612595-1267059396.png)

![pic2](http://images2015.cnblogs.com/blog/879525/201604/879525-20160425170628642-489172318.png)

 - SPARK_HOME - directory where you put Spark, like ` C:\spark-2.0.2-bin-hadoop2.7 `
 - PYTHONPATH - the python folder under Spark folder, like ` C:\spark-2.0.2-bin-hadoop2.7\python `
