#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
#
# Copyright © 2018 zack <zack@zack>
#
# Distributed under terms of the MIT license.

"""
dataframe 
['programName', 'machineName', 'materialName', 'work-center', 'orderOfProcessing', 'numberOfSheets', 'startTime', 'finishTime', 'processTime', 'Good', 'Bad', 'status', 'scheduleName', 'saveingDate/time', 'ProcessingDivision']

"""
from pyspark.conf import SparkConf
from pyspark import SparkContext
from pyspark.ml.feature import VectorAssembler
from pyspark import SparkContext
from pyspark.mllib.clustering import StreamingKMeans
from mqtt import MQTTUtils
from pyspark.streaming import StreamingContext

from datetime import datetime
#from pyspark.sql.functions import udf

brokerIP="127.0.0.1"
topic="test"
"""
input c type array
return array
"""
def replaceColumn(column):
    c = column
    for i in range(0,len(c)):
        text = c[i]
        space = 0
        while(1):
            space = text.find(" ")
            if(space==-1):
                break
            text = text[:space]+text[space+1].upper()+text[space+2:]
    #        text = text.replace(str(text[space]),str(text[space].upper()),1)
            #print (text)
        c[i] = text
    return c


def countProgramName(rdd):
    #['program name', 'order of processing', 'number of sheets', 'start time', 'finish time', 'process time', 'Good']
    rProgramName = rdd.map(lambda x:(x[0],1))
    rCount = rProgramName.reduceByKey(lambda a,b:a+b)
    print(rCount.take(2))
    return rCount.filter(lambda x:x[1]>10).collect()

    
def readCSV(rdd,sc):
    lData = rdd.map(lambda x:x.split("\n")).collect()
    #header = lData[0].split(",")
    #['program name', 'machine name', 'material name', 'work-center', 'order of processing', 'number of sheets', 'start time', 'finish time', 'process time', 'Good', 'Bad', 'status', 'schedule name', 'saveing date/time', 'Processing division']
    data = lData[1:]
    rData = sc.parallelize(data)
    s = rData.map(lambda x:x[0].split(","))
    s2 = s.map(lambda x2:[x2[0]]+ x2[4:10])
    return s2.collect()
    #return s.collect()
def toSec(aTime):
    data = aTime.asDict()
    aSec = data["process time"].split(":")
    sec = 0
    for i in range(0,len(aSec)):
        sec += int(aSec[i])*(60**(len(aSec)-i-1))
    data["processSec"]=sec
    data["avgTime"] = sec/int(data["number of sheets"])
    del data["number of sheets"]
    return Row(**data)

def initSparkConfig(conf,running,appName):
    conf.setMaster(running)
    conf.setAppName(appName)
    #conf.setLogLevel("WARN")
    return conf
def main():
    filePath="file:///home/zack/spark/learn/pyspark3/machan_laser/laser-20170802.csv"
    conf = SparkConf()
    conf = initSparkConfig(conf,"local","StreamingKMeans")
    sc = SparkContext(conf=conf)
    sc.setLogLevel("WARN")
    ssc = StreamingContext(sc,1)
    mqttData = MQTTUtils.createStream(ssc,brokerIP, topic)
    sProcessName = mqttData
    data = sc.textFile(filePath)

    lData = readCSV(data,sc)
    rData = sc.parallelize(lData)
    #['program name', 'order of processing', 'number of sheets', 'start time', 'finish time', 'process time', 'Good']
    print(rData.flatMap(lambda x:datetime.strptime(x[3],"%Y/%m/%d %H:%M:%S").take(2))
    count = countProgramName(rData)
    print (count)



if __name__=="__main__":
    main()
