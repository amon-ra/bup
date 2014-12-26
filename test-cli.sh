#!/bin/bash

python main.py save -r bup://127.0.0.1:1982 -n p1 /Users/jralfaro/Velneo
python main.py --debug save -r zmq://127.0.0.1:4242 -n p1 /Users/jralfaro/Velneo
