import sys
import socket
from libnntsc.client.nntscclient import NNTSCClient
from libnntsc.export import *

def connect_nntsc(host, port):

    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    except socket.error, msg:
        print >> sys.stderr, "Failed to create socket: %s" % (msg[1])
        return None

    try:
        s.connect((host, port))
    except socket.error, msg:
        print >> sys.stderr, "Failed to connect to %s:%d -- %s" % (host, port, msg[1])
        return None

    client = NNTSCClient(s)
    return client

def request_collections(host, port):

    client = connect_nntsc(host, port)
    if client == None:
        print >> sys.stderr, "Attempted to connect to invalid NNTSC exporter"
        return None    

    client.send_request(NNTSC_REQ_COLLECTION, -1)

    received = client.receive_message()
    if received <= 0:
        print >> sys.stderr, "Failed to get collections from NNTSC"
        client.disconnect()
        return None

    msg = client.parse_message()
    if msg[0] != NNTSC_COLLECTIONS:
        print >> sys.stderr, "Expected NNTSC_COLLECTIONS response, not %d" % (msg[0])
        client.disconnect()
        return None

    client.disconnect()
    return msg[1]['collections']

def request_streams(host, port, collection):
    streams = []

    client = connect_nntsc(host, port)
    if client == None:
        print >> sys.stderr, "Attempted to connect to invalid NNTSC exporter"
        return []
    
    client.send_request(NNTSC_REQ_STREAMS, collection)
    while 1:

        received = client.receive_message()
        if received <= 0:
            print >> sys.stderr, "Failed to get streams from NNTSC for colid %d" % (collection)
            client.disconnect()
            return []

        msg = client.parse_message()

        # Check if we got a complete parsed message, otherwise read some
        # more data
        if msg[0] == -1:
            continue
        if msg[0] != NNTSC_STREAMS:
            print >> sys.stderr, "Expected NNTSC_STREAMS response, not %d" % (msg[0])
            return []

        if msg[1]['collection'] != collection:
            continue

        streams += msg[1]['streams']
        if msg[1]['more'] == False:
            break

    client.disconnect()
    return streams


# vim: set smartindent shiftwidth=4 tabstop=4 softtabstop=4 expandtab :
