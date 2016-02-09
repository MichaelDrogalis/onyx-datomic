#!/bin/sh -x
docker run -d -p 4334-4336:4334-4336 --net="host" -e ALT_HOST=`boot2docker ip` --name datomic-free akiel/datomic-free
