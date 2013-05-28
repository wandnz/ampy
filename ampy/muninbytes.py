#!/usr/bin/env python
# -*- coding: ascii -*-

"""
Connects to a muninbytes database and interacts with it
"""

import time
import urllib2
import json
import httplib
import sys

import sqlalchemy
import ampy.result

import socket
import ampy.nntschelper
from libnntsc.export import *
from libnntsc.client.nntscclient import NNTSCClient

try:
    import pylibmc
    _have_memcache = True
except ImportError:
    _have_memcache = False

# TODO get this info by asking for the schema
aggregate_columns = ["bytes"]
group_columns = ['stream_id']

all_directions = ['received', 'sent']

class Connection(object):
    """ Class that is used to query a muninbytes dataset in NNTSC. Queries
        will return a list or a Result object that can be iterated on
    """

    def __init__(self, host="localhost", port=61234):
        """ Initialises the connection to NNTSC """
    
        self.host = host
        self.port = port
        self.invalid_connect = False
        
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

        self.streams = {}
        self.sources = {}
        self.destinations = {}
        self.sd_map = {}
        self.interfaces = {}
        self.directions = {}

        if self._load_streams() == -1:
            print >> sys.stderr, "Error loading streams for Muninbytes"
            
        
    def _load_streams(self):
        if self.invalid_connect:
            print >> sys.stderr, "Attempted to connect to invalid NNTSC exporter"
            return -1
        
        collections = ampy.nntschelper.request_collections(self.host, self.port)

        if collections == None:
            self.invalid_connect = True
            return -1
             
        self.colid = -1

        for col in collections:
            if col['module'] != 'rrd':
                continue
            if col['modsubtype'] != 'muninbytes':
                continue
            self.colid = col['id']

        if self.colid == -1:
            print >> sys.stderr, "No Munin Collection available, aborting"
            return -1

        all_streams = ampy.nntschelper.request_streams(self.host, self.port, self.colid) 

        for s in all_streams:
            self._update_stream_internal(s)
        
        return 0


    def _update_stream_internal(self, s):
        self.streams[s['stream_id']] = s
        
        if s['name'] in self.sources:
            self.sources[s['name']][s['switch']] = 1
        else:
            self.sources[s['name']] = {s['switch']:1}
        
        # TODO Update with an interface label rather than the raw number 
        # once we've added this to NNTSC
        if s['switch'] in self.interfaces:
            self.interfaces[s['switch']][s['interface']] = 1
        else:
            self.interfaces[s['switch']] = {s['interface']:1}

        if (s['switch'], s['interface']) in self.directions:
            self.directions[(s['switch'], s['interface'])][s['direction']] = 1
        else:
            self.directions[(s['switch'], s['interface'])] = {s['direction']:1}

        if s['switch'] in self.destinations:
            self.destinations[s['switch']][s['name']] = 1
        else:
            self.destinations[s['switch']] = {s['name']:1}
            
        self.sd_map[(s['switch'], s['interface'], s["direction"])] = s['stream_id']
            

    def get_sources(self, dst=None, start=None, end=None):
        """ Get all munin sources """
        return self.get_switches(dst, start, end)
       
    def get_switches(self, dst=None, start=None, end=None):
        """ Get the names of all switches that have munin data """
        if dst != None:
            if dst not in self.sources:
                return []
            return self.sources[dst].keys()
        
        sources = {}
        for v in self.sources.values():
            for src in v.keys():
                sources[src] = 1
        return sources.keys()        
    

        
    def get_destinations(self, src=None, start=None, end=None):
        """ Get all destinations from the given source """
        if src != None:
            if src not in self.destinations:
                return []
            return self.destinations[src].keys()
        
        dests = {}
        for v in self.destinations.values():
            for d in v.keys():
                dests[d] = 1
        return dests.keys()        

    def get_interfaces(self, switch):
        """ Get all available interfaces for a given switch """
        if switch != None:
            if switch not in self.interfaces:
                return []
            else:
                return self.interfaces[switch].keys()

        interfaces = {}
        for v in self.interfaces.values():
            for d in v.keys():
                interfaces[d] = 1
        return interfaces.keys()

    def get_directions(self, switch, interface):
        """ Get all available directions for a given switch / interface combo """
        if switch != None and interface != None:
            if (switch, interface) not in self.directions:
                return []
            else:
                return self.directions[(switch, interface)].keys()

        dirs = {}
        for v in self.directions.values():
            for d in v.keys():
                dirs[d] = 1
        return dirs.keys()

    def get_stream_id(self, switch, interface, direction):
        """Get the stream id matching a given switch / port combo"""
        
        if (switch, interface, direction) not in self.sd_map:
            return -1

        return self.sd_map[(switch, interface, direction)]

    def get_stream_info(self, streamid):
        """ Get more detailed and human readable information about a stream """
        # TODO figure out what extra useful information we can return
        if streamid not in self.streams:
            return {}

        return self.streams[streamid]

    def _get_recent_data(self, stream, duration, binsize, style='all'):
        
        # Default to returning only a single aggregated response
        if binsize is None:
            binsize = duration

        # If we have memcache check if this data is available already.
        if self.memcache:
            # TODO investigate why src and dst are sometimes being given to us
            # as unicode by the tooltip data requests. Any unicode string here
            # makes the result type unicode, which memcache barfs on so for now
            # force the key to be a normal string type.
            key = str("_".join([stream, str(duration), str(binsize)]))
            try:
                if key in self.memcache:
                    #print "hit %s" % key
                    return ampy.result.Result(self.memcache.get(key))
                #else:
                #    print "miss %s" % key
            except pylibmc.SomeErrors:
                # Nothing useful we can do, carry on as if data is not present.
                pass

        end = int(time.time())
        start = end - duration
        
        if style == "all":
            data = self._get_data(stream, start, end, binsize, 
                    aggregate_columns)
        else:
            data = None

        if data is None:
            # Empty list is used as a marker, because if we use None then it
            # is indistinguishable from a cache miss when we look it up. Is
            # there a better marker we can use here?
            data = []

        if self.memcache:
            try:
                self.memcache.set(key, data, self.cache_duration)
            except pylibmc.WriteError:
                # Nothing useful we can do, carry on as if data was saved.
                pass
        return ampy.result.Result(data)
        

    def get_all_recent_data(self, stream, duration, binsize=None):
        """ Fetch all result data for the most recent <duration> seconds and 
            cache it """
        return self._get_recent_data(stream, duration, binsize, "all")

    def get_all_data(self, stream, start=None, end=None, 
            binsize=60):
        """ Fetches all result data from the connection, returning a Result 
            object

            Keyword arguments:
            src -- source to get data for, or None to fetch all sources
            dst -- dest to get data for, or None to fetch all valid dests
            start -- timestamp for the start of the period to fetch data for
            end -- timestamp for the end of the period to fetch data for
            binsize -- number of seconds worth of data to bin
        """
        return self._get_period_data(stream, start, end, binsize, "all")


    def _get_period_data(self, stream, start, end, binsize, style="all"):

        # FIXME: Consider limiting maximum durations based on binsize
        # if end is not set then assume "now".
        if end is None:
            end = int(time.time())

        # If start is not set then assume 5 minutes before the end.
        if start is None:
            start = end - (60*5)

        if style == "all":
            return self._get_data(stream, start, end, binsize, 
                    aggregate_columns)
        
        return ampy.result.Result([])

    def _get_data(self, stream, start, end, binsize, columns):
        """ Fetch the data for the specified src/dst/timeperiod """
        
        if self.invalid_connect:
            print >> sys.stderr, "Attempted to connect to invalid NNTSC exporter"
            return ampy.result.Result([])

        client = ampy.nntschelper.connect_nntsc(self.host, self.port)
        if client == None:
            print >> sys.stderr, "Attempted to connect to invalid NNTSC exporter"
            self.invalid_connect = True
            return ampy.result.Result([])


        if client.request_aggregate(self.colid, [stream], start, end,
                columns, binsize, group_columns) == -1:
            return ampy.result.Result([])

        got_data = False
        streams = []
        data = []

        while not got_data:
            msg = ampy.nntschelper.get_message(client)
            if msg == None:
                break
            
            # Check if we got a complete parsed message, otherwise read some
            # more data
            if msg[0] == -1:
                continue
            
            # Look out for STREAM packets describing new streams
            if msg[0] == NNTSC_STREAMS:
                if msg[1]['collection'] != self.colid:
                    continue

                streams += msg[1]['streams']
                if msg[1]['more'] == True:
                    continue
                for s in streams:
                    self._update_stream_internal(s)
            
            if msg[0] == NNTSC_HISTORY:
                if msg[1]['collection'] != self.colid:
                    continue
                if msg[1]['streamid'] != stream:
                    continue
                if msg[1]['aggregator'] != 'avg':
                    continue
                
                data += msg[1]['data']
                if msg[1]['more'] == False:
                    got_data = True
       
        client.disconnect() 
        return ampy.result.Result(data)
        


# vim: set smartindent shiftwidth=4 tabstop=4 softtabstop=4 expandtab :
