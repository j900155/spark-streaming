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

import json
import urllib.request
import time

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

    
def toSec(time):
    s = time.split(":")
    t = 0
    for i in range(0,len(s)):
        t += int(s[i])*pow(60,len(s)-i-1)
    return t
"""
get initional data
    return the latest 10 prongramName and count bigger than 10
return array[10]
"""
def getInitData(url):
    allProgramName = []

    data = '{"aggs":{"genres":{"terms":{"field":"program name.keyword","size":10}}}}'.encode("utf-8")
    print (data)
    req = urllib.request.Request(url,data=data, headers={'content-type': 'application/json'}, \
            method="GET")
    response = ""
    getData = ""
    try:
        response = urllib.request.urlopen(req)
        getData = response.read()
    except  urllib.error.HTTPError as e:
        print(e.reason)
        print(e.headers)
    jData = json.loads(getData.decode("utf-8"))
    print (jData["aggregations"]["genres"]["buckets"])
    for i in jData["aggregations"]["genres"]["buckets"]:
            allProgramName.append(i["key"])
    print (allProgramName)
    return allProgramName
def getProcessTime(url,programName):
    query = '{"size":10,"query":{"match":{"program name":"programName"}},"_source":["start time","program name","process time"],"sort":[{"start time":{"order":"desc"}}]}'
    query = query.replace("programName",programName)
    #print ("query {}".format(query))
    data = query.encode("utf-8")
    req = urllib.request.Request(url,data=data, headers={'content-type': 'application/json'}, \
            method="GET")
    response = ""
    getData = ""
    try:
        response = urllib.request.urlopen(req)
        getData = response.read()
    except  urllib.error.HTTPError as e:
        print(e.reason)
        print(e.headers)
    jData = json.loads(getData.decode("utf-8"))
    allTime = []
    for i in jData["hits"]["hits"]:
        sTime = i["_source"]["process time"]
        allTime.append(toSec(sTime))
    print ("all time {}".format(allTime))
    return allTime

def initSparkConfig(conf,running,appName):
    conf.setMaster(running)
    conf.setAppName(appName)
    conf.set("spark.cores.max",8)
    #conf.set("spark.worker.cores",2)
    return conf

def updateData(programTime,deletProgramTime):
    newProgram = []

    return newProgram

def findProgramName(ProgramName,findName):
    index = -1
    for i in range(0,len(ProgramName)):
        if findName == ProgramName[i]:
            index = i
            break
    return index

def toVector(x):
    l = x[-1]
    v = Vectors.dense(x[:-2])
    return LabeldPoint(l,v)

def main():
    elaticIP = "10.0.0.204"
    elaticPort = 9200
    url = "http://{}:{}/machan_laser/_search".format(elaticIP,elaticPort)

    conf = SparkConf()
    #conf = initSparkConfig(conf,"local",appName="StreamingKMeans")
    conf = initSparkConfig(conf,"spark://10.0.0.202:7077",appName="StreamingKMeans")
    sc = SparkContext(conf=conf)
    sc.setLogLevel("WARN")
    ssc = StreamingContext(sc,1)

    mqttData = MQTTUtils.createStream(ssc,brokerIP, topic)

    programTime = []
    programName = getInitData(url)
    changeCount = 0

    for i in programName:
        subTime=getProcessTime(url,i)
        subTime.append(i)
        programTime.append(subTime)
    print (programTime)
    rProgramName = sc.parallelize(programTime)
    trainData = rProgramName.map(toVector)
    trainData.take(2)
    
    

if __name__=="__main__":
    main()
