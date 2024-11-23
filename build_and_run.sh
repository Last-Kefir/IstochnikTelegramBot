#!/bin/bash

ver="0.10.03"

docker rm -f tg_ci_bot

docker build -t tg_ci_bot:$ver ./

docker run --detach \
    --restart always \
    --name tg_ci_bot \
    -v "$(pwd)/log":/usr/src/app/log \
    -v "$(pwd)/config":/usr/src/app/config \
    tg_ci_bot:$ver
