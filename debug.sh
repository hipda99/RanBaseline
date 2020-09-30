#!/bin/bash

IPADDRESS=`ifconfig | grep -Eo 'inet (addr:)?([0-9]*\.){3}[0-9]*' | grep -Eo '([0-9]*\.){3}[0-9]*' | grep -v '127.0.0.1' | head -1`
echo "debuging ${IPADDRESS}:5678"
python3 -m debugpy --listen ${IPADDRESS}:5678 --wait-for-client $@
