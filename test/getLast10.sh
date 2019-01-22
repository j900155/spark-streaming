#! /bin/sh
#
# getLast10.sh
# Copyright (C) 2019 zack <zack@zack>
#
# Distributed under terms of the MIT license.
#


curl -X POST "10.0.0.204:9200/machan_laser/_search" -H 'Content-Type: application/json' -d'
{
    "sort" : [
        { "post_date" : {"order" : "asc"}},
    ],
    "query" : {
	    "term" : { "field" : "program name" }
    },
    "size":20
}
'
