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
    url = "http://{}:{}/machan_laser/_count".format(elaticIP,elaticPort)
    data = '{"query":{"term":{"program name":"2SPC201609121141"}}}'.encode("utf-8")
    print (data)
    req = urllib.request.Request(url,data=data, headers={'content-type': 'application/json'}, \
            method="GET")
    response = ""
    try:
        response = urllib.request.urlopen(req)
        print (response.read())
    except  urllib.error.HTTPError as e:
        print(e.reason)
if __name__=="__main__":
    main()
