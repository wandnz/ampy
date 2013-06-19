#!/usr/bin/env python

import time
import urllib2
import json
import httplib
import sys

import sqlalchemy
import ampy.result

import socket
from libnntsc.export import *
from libnntsc.client.nntscclient import NNTSCClient

from ampy.muninbytes_parser import MuninbytesParser
from ampy.smokeping_parser import SmokepingParser

from threading import Lock

try:
    import pylibmc
    _have_memcache = True
except ImportError:
    _have_memcache = False


class Connection(object):
    def __init__(self, host="localhost", port=61234):
        self.collections = {}
        self.collection_names = {}
        self.host = host
        self.port = port

        self.parsers = {}
        self.streams = {}

        # These locks protect our core data structures.
        #
        # ampy is often used in situations where requests may happen via 
        # multiple threads so we need to try and be thread-safe wherever
        # possible.
        self.collection_lock = Lock()
        self.parser_lock = Lock()
        self.stream_lock = Lock()

        # For now we will cache everything on localhost for 60 seconds.
        if _have_memcache:
            # TODO should cache duration be based on the amount of data?
            self.cache_duration = 60
            self.memcache = pylibmc.Client(
                    ["127.0.0.1"],
                    behaviors={
                        "tcp_nodelay": True,
                        "no_block": True,
                        })
        else:
            self.memcache = False

    def _connect_nntsc(self):
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        except socket.error, msg:
            print >> sys.stderr, "Failed to create socket: %s" % (msg[1])
            return None

        try:
            s.connect((self.host, self.port))
        except socket.error, msg:
            print >> sys.stderr, "Failed to connect to %s:%d -- %s" % (self.host, self.port, msg[1])
            return None

        client = NNTSCClient(s)
        return client
            
    def _get_nntsc_message(self, client):
        while 1:
            msg = client.parse_message()

            if msg[0] == -1:
                received = client.receive_message()
                if received <= 0:
                    print >> sys.stderr, "Failed to receive message from NNTSC"
                    client.disconnect()
                    return None
                continue

            return msg

    def _request_streams(self, colid):
        streams = []

        client = self._connect_nntsc()

        if client == None:
            print >> sys.stderr, "Unable to connect to NNTSC exporter to request streams"
            return []

        client.send_request(NNTSC_REQ_STREAMS, colid)
        while 1:

            msg = self._get_nntsc_message(client)
            if msg == None:
                return []

            # Check if we got a complete parsed message, otherwise read some
            # more data
            if msg[0] == -1:
                continue
            if msg[0] != NNTSC_STREAMS:
                print >> sys.stderr, "Expected NNTSC_STREAMS response, not %d" % (msg[0])
                return []

            if msg[1]['collection'] != colid:
                continue

            streams += msg[1]['streams']
            if msg[1]['more'] == False:
                break
        
        client.disconnect()
        return streams

    def _request_collections(self):
        client = self._connect_nntsc()
        if client == None:
            print >> sys.stderr, "Unable to connect to NNTSC exporter to request collections"
            return None

        client.send_request(NNTSC_REQ_COLLECTION, -1)

        msg = self._get_nntsc_message(client)
        if msg == None:
            return None

        if msg[0] != NNTSC_COLLECTIONS:
            print >> sys.stderr, "Expected NNTSC_COLLECTIONS response, not %d" % (msg[0])
            client.disconnect()
            return None

        client.disconnect()
        return msg[1]['collections']


    def _load_collections(self):
        collections = self._request_collections()

        if collections == None:
            return -1

        for col in collections:
            name = col['module'] + "-" + col['modsubtype']

            # TODO Make everything else use this 'name' internally, e.g. our
            # URLs contain 'rrd-smokeping' rather than 'smokeping'. This will
            # make it easier to jump to the right page when a user selects
            # a collection.

            # TODO Add nice printable names to the collection table in NNTSC
            # that we can use to populate dropdown lists / graph labels etc.
            label = name

            self.collection_lock.acquire()
            self.collections[col['id']] = {'name':name, 'label':label}
            self.collection_names[name] = col['id']
            self.collection_lock.release()
            

    def get_collections(self):
        self.collection_lock.acquire()
        if self.collections == {}:
            self.collection_lock.release()
            if self._load_collections() == -1:
                print >> sys.stderr, "Error receiving collections from NNTSC"
        else:
            self.collection_lock.release()

        # XXX This is outside of the lock so could be subject to a race
        # condition, but we shouldn't touch the collections often so hopefully
        # this won't be too much of an issue
        return self.collections;
        
    def create_parser(self, name):
        parser = None

        self.parser_lock.acquire()
        if name in self.parsers:
            self.parser_lock.release()
            return
        self.parser_lock.release()

        self.collection_lock.acquire()
        if self.collections == {}:
            self.collection_lock.release()
            if self._load_collections() == -1:
                print >> sys.stderr, "Error receiving collections from NNTSC"
                return
            self.collection_lock.acquire()
             
        if name not in self.collection_names.keys():
            print >> sys.stderr, "No NNTSC collection matching %s" % (name)
            return
        else:
            colid = self.collection_names[name]
        self.collection_lock.release()
        
        streams = self._request_streams(colid)
        
        if name == "rrd-smokeping":
            parser = SmokepingParser()
            self.parser_lock.acquire()
            self.parsers["rrd-smokeping"] = parser 
            self.parser_lock.release()

        if name == "rrd-muninbytes":
            parser = MuninbytesParser()
            self.parser_lock.acquire()
            self.parsers["rrd-muninbytes"] = parser 
            self.parser_lock.release()

        if parser != None:
            self._update_stream_map(streams, parser, colid)

    def _update_stream_map(self, streams, parser, colid):

        for s in streams:
            self.stream_lock.acquire()
            self.streams[s['stream_id']] = {'parser':parser, 'streaminfo':s,
                    'collection':colid}
            self.stream_lock.release()
            parser.add_stream(s)

    def _process_new_streams(self, msg):
        
        # XXX Dunno if this actually works - kinda tricky to test
        colid = msg[1]['collection']

        if colid not in self.collections:
            return
        
        self.parser_lock.acquire()
        parser = self.parsers[self.collections[colid]['name']]
        self.parser_lock.release()

        self._update_stream_map(msg[1]['streams'], parser, colid) 

    def get_selection_options(self, name, params):
        
        self.parser_lock.acquire()
        if not self.parsers.has_key(name):
            return []
        
        parser = self.parsers[name]
        self.parser_lock.release()

        return parser.get_selection_options(params)

    def get_stream_info(self, streamid):
        self.stream_lock.acquire()
        if streamid not in self.streams:
            return {}
            
        info = self.streams[streamid]['streaminfo']
        self.stream_lock.release()
        return info

    def get_stream_id(self, name, params):
        
        self.parser_lock.acquire()
        if not self.parsers.has_key(name):
            return -1
        parser = self.parsers[name]
        self.parser_lock.release()
        
        return parser.get_stream_id(params)


    def get_recent_data(self, stream, duration, binsize, detail):
        self.stream_lock.acquire()
        if stream not in self.streams:
            print >> sys.stderr, "Requested data for unknown stream: %d" % (stream)
            self.stream_lock.release()
            return ampy.result.Result([])    
        self.stream_lock.release()

        # Default to returning only a single aggregated response
        if binsize is None:
            binsize = duration
       
        if detail is None:
            detail = "full"
        
        end = int(time.time())
        start = end - duration

        # If we have memcache check if this data is available already.
        if self.memcache:
            # TODO investigate why src and dst are sometimes being given to us
            # as unicode by the tooltip data requests. Any unicode string here
            # makes the result type unicode, which memcache barfs on so for now
            # force the key to be a normal string type.
            key = str("_".join([str(stream), str(start), str(end), 
                    str(binsize), str(detail) ]))
            try:
                if key in self.memcache:
                    #print "hit %s" % key
                    return ampy.result.Result(self.memcache.get(key))
                #else:
                #    print "miss %s" % key
            except pylibmc.SomeErrors:
                # Nothing useful we can do, carry on as if data is not present.
                pass

        return self._get_data(stream, start, end, binsize, detail)

    def get_period_data(self, stream, start, end, binsize, detail):
        self.stream_lock.acquire()
        if stream not in self.streams:
            print "Requested data for unknown stream: %d" % (stream)
            self.stream_lock.release()
            return ampy.result.Result([])    
        self.stream_lock.release()
        
        # FIXME: Consider limiting maximum durations based on binsize
        # if end is not set then assume "now".
        if end is None:
            end = int(time.time())

        # If start is not set then assume 5 minutes before the end.
        if start is None:
            start = end - (60*5)
       
        if detail is None:
            detail = "full"
        
        
        # If we have memcache check if this data is available already.
        if self.memcache:
            # TODO investigate why src and dst are sometimes being given to us
            # as unicode by the tooltip data requests. Any unicode string here
            # makes the result type unicode, which memcache barfs on so for now
            # force the key to be a normal string type.
            key = str("_".join([str(stream), str(start), str(end), 
                    str(binsize), str(detail)]))
            try:
                if key in self.memcache:
                    #print "hit %s" % key
                    return ampy.result.Result(self.memcache.get(key))
                #else:
                #    print "miss %s" % key
            except pylibmc.SomeErrors:
                # Nothing useful we can do, carry on as if data is not present.
                pass

        return self._get_data(stream, start, end, binsize, detail)

    def _get_data(self, stream, start, end, binsize, detail):

        self.stream_lock.acquire()
        parser = self.streams[stream]['parser']
        colid = self.streams[stream]['collection']
        self.stream_lock.release()
        
        agg_columns = parser.get_aggregate_columns(detail)
        group_columns = parser.get_group_columns()

        if parser == None:
            print >> sys.stderr, "Cannot fetch data -- no valid parser for stream %s" % (stream)     
            return ampy.result.Result([])    
        
        client = self._connect_nntsc()
        if client == None:
            print >> sys.stderr, "Cannot fetch data -- lost connection to NNTSC"
            return ampy.result.Result([])    
            
        
        if client.request_aggregate(colid, [stream], start, end,
                agg_columns, binsize, group_columns) == -1:
            return ampy.result.Result([])    

        got_data = False
        data = []

        while not got_data:
            msg = self._get_nntsc_message(client)
            if msg == None:
                break

            # Check if we got a complete parsed message, otherwise read some
            # more data
            if msg[0] == -1:
                continue

            # Look out for STREAM packets describing new streams
            if msg[0] == NNTSC_STREAMS:
                self._process_new_streams(msg[1])

            if msg[0] == NNTSC_HISTORY:
                # Sanity checks
                if msg[1]['collection'] != colid:
                    continue
                if msg[1]['streamid'] != stream:
                    continue
                if msg[1]['aggregator'] != 'avg':
                    continue

                data += msg[1]['data']
                if msg[1]['more'] == False:
                    got_data = True
        client.disconnect()
        
        data = parser.format_data(data)
        key = str("_".join([str(stream), str(start), str(end), str(binsize),
                str(detail)]))
        
        if self.memcache:
            try:
                self.memcache.set(key, data, self.cache_duration)
            except pylibmc.WriteError:
                # Nothing useful we can do, carry on as if data was saved.
                pass

        return ampy.result.Result(data)




# vim: set smartindent shiftwidth=4 tabstop=4 softtabstop=4 expandtab :
