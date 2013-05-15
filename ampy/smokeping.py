#!/usr/bin/env python
# -*- coding: ascii -*-

"""
Connects to a smokeping database and interacts with it
"""

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

try:
    import pylibmc
    _have_memcache = True
except ImportError:
    _have_memcache = False

# TODO get this info by asking for the schema
aggregate_columns = ['uptime', 'loss', 'median',
    'ping1', 'ping2', 'ping3', 'ping4', 'ping5', 'ping6', 'ping7', 'ping8',
    'ping9', 'ping10', 'ping11', 'ping12', 'ping13', 'ping14', 'ping15', 
    'ping16', 'ping17', 'ping18', 'ping19', 'ping20']
group_columns = ['stream_id']


class Connection(object):
    """ Class that is used to query a smokeping dataset in NNTSC. Queries
        will return a list or a Result object that can be iterated on
    """

    def __init__(self, host="localhost", port=61234):
        """ Initialises the connection to NNTSC """

        self.streams = {}
        self.sources = {}
        self.destinations = {}
        self.sd_map = {}
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

        if self._load_streams() == -1:
            print >> sys.stderr, "Error loading streams for Smokeping"

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
            if col['modsubtype'] != 'smokeping':
                continue
            self.colid = col['id']

        if self.colid == -1:
            print >> sys.stderr, "No Smokeping Collection available, aborting"
            return -1

        all_streams = ampy.nntschelper.request_streams(self.host, self.port, self.colid)

        for s in all_streams:
            self._update_stream_internal(s)

        return 0

    

    def _update_stream_internal(self, s):
        self.streams[s['stream_id']] = s
        
        if s['host'] in self.sources:
            self.sources[s['host']][s['source']] = 1
        else:
            self.sources[s['host']] = {s['source']:1}
        
        if s['source'] in self.destinations:
            self.destinations[s['source']][s['host']] = 1
        else:
            self.destinations[s['source']] = {s['host']:1}
            
        self.sd_map[(s['source'], s['host'])] = s['stream_id']
            

    def get_sources(self, dst=None, start=None, end=None):
        """ Get all smokeping sources """
      
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

    def get_stream_id(self, src, dest):
        """Get the stream id matching a given source / dest combo"""
        
        if (src, dest) not in self.sd_map:
            return -1

        return self.sd_map[(src, dest)]

    def get_stream_info(self, streamid):
        """ Get more detailed and human readable information about a stream """
        # TODO figure out what extra useful information we can return
        if streamid not in self.streams:
            return {}

        return self.streams[streamid]

    def _get_recent_data(self, src, dst, duration, binsize, style='all'):
        
        # Default to returning only a single aggregated response
        if binsize is None:
            binsize = duration

        # If we have memcache check if this data is available already.
        if self.memcache:
            # TODO investigate why src and dst are sometimes being given to us
            # as unicode by the tooltip data requests. Any unicode string here
            # makes the result type unicode, which memcache barfs on so for now
            # force the key to be a normal string type.
            key = str("_".join([src, dst, str(duration), str(binsize)]))
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
            data = self._get_data(src, dst, start, end, binsize, 
                    aggregate_columns)
        elif style == "basic":
            data = self._get_data(src, dst, start, end, binsize, 
                    ["median", "loss"])
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
        

    def get_all_recent_data(self, src, dst, duration, binsize=None):
        """ Fetch all result data for the most recent <duration> seconds and 
            cache it """
        return self._get_recent_data(src, dst, duration, binsize, "all")

    def get_basic_recent_data(self, src, dst, duration, binsize=None):
        """ Fetch just the loss and median data for the most recent <duration> 
            seconds and cache it """
        return self._get_recent_data(src, dst, duration, binsize, "basic")

    def get_all_data(self, src=None, dst=None, start=None, end=None, 
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
        return self._get_period_data(src, dst, start, end, binsize, "all")

    def get_basic_data(self, src=None, dst=None, start=None, end=None, 
            binsize=60):
        """ Fetches the median and loss data for a smokeping stream, 
            returning a Result object

            Keyword arguments:
            src -- source to get data for, or None to fetch all sources
            dst -- dest to get data for, or None to fetch all valid dests
            start -- timestamp for the start of the period to fetch data for
            end -- timestamp for the end of the period to fetch data for
            binsize -- number of seconds worth of data to bin
        """
        return self._get_period_data(src, dst, start, end, binsize, "basic")

    def _get_period_data(self, src, dst, start, end, binsize, style="all"):
        if src is None:
            # Pass through other args so we can do smart filtering?
            return self.get_sources(start=start, end=end)

        if dst is None:
            # Pass through other args so we can do smart filtering?
            return self.get_destinations(src, start, end)

        # FIXME: Consider limiting maximum durations based on binsize
        # if end is not set then assume "now".
        if end is None:
            end = int(time.time())

        # If start is not set then assume 5 minutes before the end.
        if start is None:
            start = end - (60*5)

        if style == "all":
            return self._get_data(src, dst, start, end, binsize, 
                    aggregate_columns)
        elif style == "basic":
            return self._get_data(src, dst, start, end, binsize, 
                    ['loss', 'median'])
        
        return ampy.result.Result([])

    def _get_data(self, src, dst, start, end, binsize, columns):
        """ Fetch the data for the specified src/dst/timeperiod """
       
        if (src,dst) not in self.sd_map:
            return ampy.result.Result([])

        stream = self.sd_map[(src,dst)]

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
        formatted = []
                
        for d in data:
            newdict = {}
            pings = [None] * 20
            for k, v in d.items():
                
                if "ping" in k:
                    index = int(k.split("ping")[1]) - 1
                    assert(index >= 0 and index < 20)
                    pings[index] = v
                else:
                    newdict[k] = v
           
            newdict["pings"] = pings

            formatted.append(newdict)

        #print formatted[0]
        #print formatted[-1]
                    
        return ampy.result.Result(formatted)
        


# vim: set smartindent shiftwidth=4 tabstop=4 softtabstop=4 expandtab :
