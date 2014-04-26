import zmq
import time

context = zmq.Context()
host = 'localhost'
port = 7700


def stop():
    req = context.socket(zmq.REQ)
    req.connect('tcp://%s:%s' % (host, port))
    # Sleep before sending, or the first message maybe lost.
    # ref: http://lists.zeromq.org/pipermail/zeromq-dev/2013-July/022246.html
    time.sleep(1)

    msg = 'stop' + ' {"action": "stop"}'
    req.send(msg)
    print req.recv()


if __name__ == '__main__':
    stop()
