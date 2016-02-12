#!/bin/sh -x
docker run -d -p 4334-4336:4334-4336 --net="host" -e ALT_HOST=$1 --name datomic-free akiel/datomic-free
