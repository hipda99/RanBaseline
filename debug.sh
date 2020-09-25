#!/bin/bash

python3 -m debugpy --listen 10.50.64.207:5678 --wait-for-client $@
