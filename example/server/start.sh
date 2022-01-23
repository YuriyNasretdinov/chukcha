#!/bin/sh

set -e
set -x

killall chukcha || true
sleep 0.1

cd $(dirname $0)

cd ../..
pwd
go install -v ./...

chukcha -cluster MotherRussia -dirname ~/chukcha-data/voronezh/ -instance Voronezh -listen 127.0.0.1:8081 -rotate-chunk-interval=10s &
chukcha -cluster MotherRussia -dirname ~/chukcha-data/moscow -instance Moscow -listen 127.0.0.1:8080 -rotate-chunk-interval=10s &

wait
