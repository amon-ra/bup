import sys
import zmq

context = zmq.Context()

s1 = context.socket(zmq.ROUTER)
s2 = context.socket(zmq.DEALER)
s1.bind(sys.argv[1])
s2.bind(sys.argv[2])
zmq.device(zmq.QUEUE, s1, s2)

