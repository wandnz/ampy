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

from ampy.muninbytes import MuninbytesParser
from ampy.lpibytes import LPIBytesParser
from ampy.smokeping import SmokepingParser

from threading import Lock

try:
    import pylibmc
    _have_memcache = True
except ImportError:
    _have_memcache = False


class Connection(object):
    """ Class that is used to query NNTSC. Will store information about
        collections and streams internally so that queries for them can be
        handled without contacting the database again.

        Queries for measurement data will return a Result object that can be
        iterated on.

        API Function Names
        ------------------
        create_parser:
            creates a parser that is used for querying a given collection
        get_collections:
            returns the list of available collections
        get_stream_info:
            returns a dictionary describing a given stream
        get_stream_id:
            finds and returns the id of the stream matching a given description
        get_recent_data:
            queries NNTSC for the most recent data going back N seconds
        get_period_data:
            queries NNTSC for measurement data over a specified time period
        get_selection_options:
            returns a list of terms for populating a dropdown list for 
            selecting a stream, based on what has already been selected
    """
    def __init__(self, host="localhost", port=61234):
        """ Initialises the Connection class

            Parameters:
              host -- the host that NNTSC is running on
              port -- the port to connect to on the NNTSC host
        """
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
        """ Connects to NNTSC and returns a NNTSCClient that can be used to
            easily make requests and receive responses
        """
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
        """ Receives and parses a message from NNTSC 
        
            Parameters:
              client -- the NNTSCClient that was used to make the original
                        request   
        """
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
        """ Query NNTSC for all of the streams for a given collection

            Parameters:
              colid -- the id number of the collection to query for (not the
                       name!)

            Returns:
              a list of streams
        """
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
        """ Query NNTSC for all of the available collections

            Returns:
              a list of collections
        """
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
        """ Acquire a list of all collections from NNTSC and store them
            locally for future requests
        """
        collections = self._request_collections()

        if collections == None:
            return -1

        for col in collections:
            name = col['module'] + "-" + col['modsubtype']

            # TODO Add nice printable names to the collection table in NNTSC
            # that we can use to populate dropdown lists / graph labels etc.
            label = name

            self.collection_lock.acquire()
            self.collections[col['id']] = {'name':name, 'label':label}
            self.collection_names[name] = col['id']
            self.collection_lock.release()
            

    def get_collections(self):
        """ API function for requesting the list of available collections.

            If we don't have a local copy, query NNTSC for the collections
            and then save the results for subsequent requests. Otherwise, 
            return the saved collection list.
        """
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
        """ Creates a 'parser' for the named collection.
        
            A parser is necessary for being able to query NNTSC for data about
            the collection or any streams belonging to it.
            
            If a parser for the named collection already exists, this function
            will immediately return -- this means you don't have to worry
            about only calling create_parser once for each collection; call it
            before doing any queries.
            
            Otherwise, this function will create a new parser object for
            the requested collection and query NNTSC for all of the streams
            belonging to that collection. Details about the streams are
            saved locally and also passed into the new parser to allow it to
            construct its own internal data structures for fast lookups. 

            Params:
              name -- the name of the collection to create a parser for (not
                      the ID number!)
        """
        parser = None

        # If this parser already exists, we can just use that
        self.parser_lock.acquire()
        if name in self.parsers:
            self.parser_lock.release()
            return
        self.parser_lock.release()

        # Grab the collections if we haven't already
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
        
        # Get the streams for the requested collection 
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
        
        if name == "lpi-bytes":
            parser = LPIBytesParser()
            self.parser_lock.acquire()
            self.parsers["lpi-bytes"] = parser 
            self.parser_lock.release()
            


        if parser != None:
            self._update_stream_map(streams, parser, colid)

    def _update_stream_map(self, streams, parser, colid):
        """ Adds a list of streams to the internal stream map.

            Also passes each stream to the provided parser, so it can update
            its own internal maps.

            Parameters:
              streams -- a list of streams to be added to the stream map
              parser -- the parser for the collection that the streams belong
                        to
              colid -- the id number of the collection that the streams belong
                        to
        """
        for s in streams:
            self.stream_lock.acquire()
            self.streams[s['stream_id']] = {'parser':parser, 'streaminfo':s,
                    'collection':colid}
            self.stream_lock.release()
            parser.add_stream(s)

    def _process_new_streams(self, msg):
        """ Processes a NNTSC_STREAMS message and updates the internal maps
            to include the new streams.

            Parameters:
              msg -- the received NNTSC_STREAMS message
        """

        # XXX Dunno if this actually works - kinda tricky to test
        colid = msg[1]['collection']

        if colid not in self.collections:
            return
        
        self.parser_lock.acquire()
        if self.collections[colid]['name'] not in self.parsers:
            self.parser_lock.release()
            return
        parser = self.parsers[self.collections[colid]['name']]
        self.parser_lock.release()

        self._update_stream_map(msg[1]['streams'], parser, colid) 

    def get_selection_options(self, name, params):
        """ Given a known set of stream parameters, return a list of possible
            values that can be used to select a valid stream.
            
            This method is mainly used for populating dropdown lists in
            amp-web. An example use case: the collection is rrd-smokeping and
            the user has selected a source using the dropdown list. We call
            this function with params = {'source': <the selected value>} and
            it will return a list of valid targets for that source.

            The correct keys for the params directory will vary depending on
            the collection being queried. See the documentation of this
            function within each parser for a list of supported parameters.

            Params:
              name -- the name of the collection being queried
              params -- a dictionary describing the parameters that have
                        already been selected. 

            Returns:
              a list of valid values for a subsequent selection option, given
              the provided earlier selections.
        """
        self.parser_lock.acquire()
        if not self.parsers.has_key(name):
            return []
        
        parser = self.parsers[name]
        self.parser_lock.release()

        # This is all handled within the parser, as the parameters that can
        # be used as selection options will differ from collection to 
        # collection.
        return parser.get_selection_options(params)

    def get_stream_info(self, streamid):
        """ Returns the stream information dictionary for a given stream. 
        
            Parameters:
              streamid -- the id of the stream that the info is requested for
        """
        self.stream_lock.acquire()
        if streamid not in self.streams:
            return {}
            
        info = self.streams[streamid]['streaminfo']
        self.stream_lock.release()
        return info

    def get_stream_id(self, name, params):
        """ Finds the ID of the stream that matches the provided parameters.

            To be successful, the params dictionary must contain all of the
            possible selection parameters for the collection. For example,
            a rrd-muninbytes stream ID will only be found if the params
            dictionary contains 'switch', 'interface' AND 'direction'. 

            See also get_selection_options().

            Parameters:
              name -- the name of the collection to search for the stream
              params -- a dictionary containing parameters describing the
                        stream that is being searched for. All possible
                        identifying parameters must be present to find the
                        stream.

            Returns:
              the id number of the stream that is uniquely identified by
              the given parameters. -1 is returned if a unique match was not
              possible.
        """
        self.parser_lock.acquire()
        if not self.parsers.has_key(name):
            return -1
        parser = self.parsers[name]
        self.parser_lock.release()
        
        return parser.get_stream_id(params)


    def get_recent_data(self, stream, duration, binsize, detail):
        """ Returns data measurements for a time period starting at 'now' and
            going back a specified number of seconds.

            See also get_period_data().

            The detail parameter allows the user to limit the amount of data
            returned to them. For example, smokeping results store the
            latency measurements for each individual ping. Requesting "full"
            detail would get these individual results along with the median,
            uptime and loss measurements. "minimal" will only return median
            and loss: enough to produce a simple latency time series graph
            and much smaller to send.

            Note that for some collections, only "full" detail may be available.
            In those cases, specifying any other level of detail will simply
            result in the "full" data being returned regardless.

            Valid values for the detail parameter are:
                "full" -- return all available measurements
                "minimal" -- return a minimal set of measurements
    
            Parameters:
              stream -- the id number of the stream to fetch data for
              duration -- the length of the time period to fetch data for, in
                          seconds
              binsize -- the frequency at which data should be aggregated. If
                         None, the binsize is assumed to be the duration.
              detail -- a string that describes the level of measurement detail 
                        that should be returned for each datapoint. If None,
                        assumed to be "full". 

            Returns:
              an ampy Result object containing all of the requested measurement
              data. If the request fails, this Result object will be empty.
        """
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
        """ Returns data measurements for a time period explicitly described
            using a start and end time.

            See also get_recent_data().
            
            The detail parameter allows the user to limit the amount of data
            returned to them. For example, smokeping results store the
            latency measurements for each individual ping. Requesting "full"
            detail would get these individual results along with the median,
            uptime and loss measurements. "minimal" will only return median
            and loss: enough to produce a simple latency time series graph
            and much smaller to send.

            Note that for some collections, only "full" detail may be available.
            In those cases, specifying any other level of detail will simply
            result in the "full" data being returned regardless.

            Valid values for the detail parameter are:
                "full" -- return all available measurements
                "minimal" -- return a minimal set of measurements
    
            Parameters:
              stream -- the id number of the stream to fetch data for
              start -- the starting point of the time period, in seconds since
                       the epoch. If None, assumed to be 5 minutes before 'end'.
              end -- the end point of the time period, in seconds since the
                     epoch. If None, assumed to be 'now'.
              binsize -- the frequency at which data should be aggregated. If
                         None, the binsize is assumed to be the duration.
              detail -- a string that describes the level of measurement detail 
                        that should be returned for each datapoint. If None,
                        assumed to be "full". 

            Returns:
              an ampy Result object containing all of the requested measurement
              data. If the request fails, this Result object will be empty.
        """
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
        """ Internal function that actually performs the NNTSC query to get
            measurement data, parses the responses and forms up the ampy
            Result object to be returned to the caller.

            Parameters:
                the same as get_period_data()

            Returns:
                the same as get_period_data()
        """
    
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
       
        
        # Some collections have some specific formatting they like to do to
        # the data before displaying it, e.g. rrd-smokeping combines the ping 
        # data into a single list rather than being 20 separate dictionary 
        # entries. 
        data = parser.format_data(data, stream, self.streams[stream]['streaminfo'])
       
        key = str("_".join([str(stream), str(start), str(end), str(binsize),
                str(detail)]))
        
        # Save the data in the cache
        if self.memcache:
            try:
                self.memcache.set(key, data, self.cache_duration)
            except pylibmc.WriteError:
                # Nothing useful we can do, carry on as if data was saved.
                pass

        return ampy.result.Result(data)




# vim: set smartindent shiftwidth=4 tabstop=4 softtabstop=4 expandtab :
