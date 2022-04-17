#!/bin/sh

set -x

ssh -R8080:localhost:8080 -R8081:localhost:8081 -R8083:localhost:8083 -R8084:localhost:8084 -f -N z
ssh -L8082:127.0.0.1:8082 -f -N z

ssh -R8080:localhost:8080 -R8081:localhost:8081 -R8082:localhost:8082  -R8084:localhost:8084 -f -N g
ssh -L8083:127.0.0.1:8083 -f -N g

ssh -R8080:localhost:8080 -R8081:localhost:8081 -R8082:localhost:8082 -R8083:localhost:8083 -f -N a
ssh -L8084:127.0.0.1:8084 -f -N a
