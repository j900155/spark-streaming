#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
#
# Copyright © 2019 zack <zack@zack>
#
# Distributed under terms of the MIT license.

"""

"""

import json
import urllib.request
#import urllib
import time

def main():
    elaticIP = "10.0.0.204"
    elaticPort = 9200
    url = "http://{}:{}/machan_laser/_search".format(elaticIP,elaticPort)

    order = {"order":"desc"}
    sort = {"start time": order}
    terms = {"field":"program name", "size":20}
    langs = {"terms":terms,"sort":sort}
    l = {"langs":langs}
    aggs = {"aggs":l}
    print (aggs)
    #data = json.dumps(aggs).encode("utf-8")
    #data = '{"aggs":{"genres":{"terms":{"field":"program name"}}}}'.encode("utf-8")
    data = '{"aggs":{"genres":{"terms":{"field":"program name.keyword","size":10}}}}'.encode("utf-8")
    print(url)
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
    #print (jData)
    print ("==========")
    print (jData["aggregations"]["genres"]["buckets"])
if __name__=="__main__":
    main()
