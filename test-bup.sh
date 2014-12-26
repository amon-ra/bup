#!/bin/bash

export BUP_DIR='/tmp/bup-srv'
rm -rf $BUP_DIR
python -d main.py --debug init
python -d main.py --debug daemon

