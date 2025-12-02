#!/bin/bash

echo "Starting Xvfb..."
Xvfb :0 -screen 0 1024x768x16 &

export DISPLAY=:0

sleep 2

echo "Starting IB Gateway..."
/root/Jts/ibgateway/1010/ibgateway \
    &

sleep 5

echo "Starting IBC..."
/opt/ibc/scripts/ibcstart.sh \
    /root/Jts \
    -gateway \
    -config /opt/ibc/config.ini \
    -mode=${IBC_TRADING_MODE} \
    -ibc-path /opt/ibc \
    -login=${IBC_LOGIN} \
    -password=${IBC_PASSWORD}

wait
