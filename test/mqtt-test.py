#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
#
# Copyright © 2018 zack <zack@zack>
#
# Distributed under terms of the MIT license.

from pyspark.conf import SparkConf
from pyspark import SparkContext
from pyspark.mllib.clustering import StreamingKMeans
from mqtt import MQTTUtils
from pyspark.streaming import StreamingContext

from datetime import datetime

import json
import urllib.request
import time

#from pyspark.sql.functions import udf

brokerIP="tcp://127.0.0.1"
topic="test"

def initSparkConfig(conf,running,appName):
    conf.setMaster(running)
    conf.setAppName(appName)
    conf.set("spark.cores.max",16)
    #conf.set("spark.worker.cores",2)
    return conf


def main():
    conf = SparkConf()
    conf = initSparkConfig(conf,"local[3]",appName="mqttTest")
    #conf = initSparkConfig(conf,"spark://10.0.0.202:7077",appName="StreamingKMeans")
    sc = SparkContext(conf=conf)
    sc.setLogLevel("WARN")
    ssc = StreamingContext(sc,3)

    mqttData = MQTTUtils.createStream(ssc,brokerIP, topic)
    r = mqttData.map(lambda x:x+"aa")
    r.pprint()
    ssc.start()
    ssc.awaitTermination()
if __name__=="__main__":
    main()
