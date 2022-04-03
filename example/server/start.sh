#!/bin/sh

set -e
set -x

killall chukcha || true
ssh z killall chukcha || true
ssh g killall chukcha || true
ssh a killall chukcha || true
sleep 0.1

cd $(dirname $0)

cd ../..
pwd
go install -v ./...
GOARCH=arm go install -v ./...
scp ~/go/bin/chukcha z:
scp ~/go/bin/chukcha g:
scp ~/go/bin/linux_arm/chukcha a:

PEERS="Moscow=127.0.0.1:8080,Voronezh=127.0.0.1:8081,Peking=127.0.0.1:8082,Bengaluru=127.0.0.1:8083,Phaenus=127.0.0.1:8084"
COMMON_PARAMS="-cluster AllComrads -rotate-chunk-interval=10s -peers=$PEERS"

# ALL NODES ARE EQUAL
chukcha $COMMON_PARAMS -dirname ~/chukcha-data/moscow -instance Moscow -listen 127.0.0.1:8080 &
chukcha $COMMON_PARAMS -dirname ~/chukcha-data/voronezh/ -instance Voronezh -listen 127.0.0.1:8081 &
ssh z ./chukcha $COMMON_PARAMS -dirname ./chukcha-data -instance Peking -listen 127.0.0.1:8082 &
ssh g ./chukcha $COMMON_PARAMS -dirname ./chukcha-data -instance Bengaluru -listen 127.0.0.1:8083 &
ssh a ./chukcha $COMMON_PARAMS -dirname ./chukcha-data -instance Phaenus -listen 127.0.0.1:8084 &

wait
