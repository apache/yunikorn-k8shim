#!/bin/sh

yarn install

rm -rf ./dist

yarn build:prod

docker build -t yunikorn/scheduler-web:0.1.0 -f ./nginx/Dockerfile .

docker run -d -p 8089:80 yunikorn/scheduler-web:0.1.0