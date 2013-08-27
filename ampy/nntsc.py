#!/usr/bin/env python

import time
import urllib2
import json
import httplib
import sys

import sqlalchemy
import ampy.result

import socket, os
from libnntsc.export import *
from libnntsc.client.nntscclient import NNTSCClient

from ampy.muninbytes import MuninbytesParser
from ampy.lpibytes import LPIBytesParser
from ampy.smokeping import SmokepingParser
from ampy.lpipackets import LPIPacketsParser
from ampy.lpiflows import LPIFlowsParser
from ampy.lpiusers import LPIUsersParser
from ampy.ampicmp import AmpIcmpParser
from ampy.amptraceroute import AmpTracerouteParser


from threading import Lock

try:
    import pylibmc
    from ampy.caching import AmpyCache
    _have_memcache = True
except ImportError:
    _have_memcache = False

STREAM_CHECK_FREQUENCY = 60 * 5

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
        get_collection_streams:
            returns a list of stream information dictionaries describing all
            of the streams that belong to a particular collection
        get_related_streams:
            returns a list of streams from other collections that share
            common properties with a given stream
    """
    def __init__(self, host="localhost", port=61234, ampconfig=None):
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

        self.ampdbconfig = {}

        # Store the configuration for the amp2 metadata db, if possible
        if ampconfig != None:
            if 'host' in ampconfig:
                self.ampdbconfig['host'] = ampconfig['host']
            else:
                self.ampdbconfig['host'] = None
            
            if 'user' in ampconfig:
                self.ampdbconfig['user'] = ampconfig['user']
            else:
                self.ampdbconfig['user'] = None

            if 'pwd' in ampconfig:
                self.ampdbconfig['pwd'] = ampconfig['pwd']
            else:
                self.ampdbconfig['pwd'] = None

        # These locks protect our core data structures.
        #
        # ampy is often used in situations where requests may happen via
        # multiple threads so we need to try and be thread-safe wherever
        # possible.
        self.collection_lock = Lock()
        self.parser_lock = Lock()

        # For now we will cache everything on localhost for 60 seconds.
        if _have_memcache:
            self.memcache = AmpyCache(12)
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

    def _request_streams(self, colid, startingfrom):
        """ Query NNTSC for all of the streams for a given collection

            Parameters:
              colid -- the id number of the collection to query for (not the
                       name!)
              startingfrom -- the id of the last stream received for this
                              collection, so you can ask for only new streams.
                              If 0, you'll get all streams for the collection.

            Returns:
              a list of streams
        """
        streams = []
        client = self._connect_nntsc()

        if client == None:
            print >> sys.stderr, "Unable to connect to NNTSC exporter to request streams"
            return []

        client.send_request(NNTSC_REQ_STREAMS, colid, startingfrom)
        while 1:

            msg = self._get_nntsc_message(client)
            if msg == None:
                client.disconnect()
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
            client.disconnect()
            return None

        if msg[0] != NNTSC_COLLECTIONS:
            print >> sys.stderr, "Expected NNTSC_COLLECTIONS response, not %d" % (msg[0])
            client.disconnect()
            return None

        client.disconnect()
        return msg[1]['collections']

    def _lookup_parser(self, name):
        self.parser_lock.acquire()
        if not self.parsers.has_key(name):
            self.parser_lock.release()
            return None
        parser = self.parsers[name]
        self.parser_lock.release()
        
        return parser


    def _lookup_collection(self, name):
        self.collection_lock.acquire()
        if self._load_collections() == -1:
            print >> sys.stderr, "Unable to load collections for %s" % (name)
            self.collection_lock.release()
            return None, None

        if name not in self.collection_names.keys():
            print >> sys.stderr, "No NNTSC collection matching %s" % (name)
            self.collection_lock.release()
            return None, None
        else:
            colid = self.collection_names[name]
            coldata = self.collections[colid]

        self.collection_lock.release()
        return colid, coldata        

    def _load_collections(self):
        """ Acquire a list of all collections from NNTSC and store them
            locally for future requests
        """
        if self.collections != {}:
            return
        
        collections = self._request_collections()

        if collections == None:
            return -1

        for col in collections:
            name = col['module'] + "-" + col['modsubtype']

            # TODO Add nice printable names to the collection table in NNTSC
            # that we can use to populate dropdown lists / graph labels etc.
            label = name
            self.collections[col['id']] = {'name':name, 'label':label, 'laststream':0, 'lastchecked':0, 'streamlock':Lock(), 'module':col['module']}
            self.collection_names[name] = col['id']


    def get_collections(self):
        """ API function for requesting the list of available collections.

            If we don't have a local copy, query NNTSC for the collections
            and then save the results for subsequent requests. Otherwise,
            return the saved collection list.
        """
        self.collection_lock.acquire()
        if self._load_collections() == -1:
            print >> sys.stderr, "Error receiving collections from NNTSC"
        self.collection_lock.release()

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

        if name == "amp-icmp":
            parser = AmpIcmpParser(self.ampdbconfig)

        if name == "amp-traceroute":
            parser = AmpTracerouteParser(self.ampdbconfig)

        if name == "rrd-smokeping":
            parser = SmokepingParser()

        if name == "rrd-muninbytes":
            parser = MuninbytesParser()

        if name == "lpi-bytes":
            parser = LPIBytesParser()

        if name == "lpi-flows":
            parser = LPIFlowsParser()

        if name == "lpi-packets":
            parser = LPIPacketsParser()

        if name == "lpi-users":
            parser = LPIUsersParser()

        if parser != None:
            self.parsers[name] = parser
        self.parser_lock.release()

    def _update_stream_map(self, collection, parser):
        """ Asks NNTSC for any streams that we don't know about and adds
            them to our internal stream map.

            Also passes each stream to the provided parser, so it can update
            its own internal maps.

            Parameters:
                collection -- the name of the collection to request streams for
                parser -- the parser to push the new streams to
        """
       
        colid, coldata = self._lookup_collection(collection)
        
        if colid == None:
            return
        
        laststream = coldata['laststream']
        lastchecked = coldata['lastchecked']
        streamlock = coldata['streamlock']

        now = time.time()
        if now < (lastchecked + STREAM_CHECK_FREQUENCY):
            return

        # Check if there are any new streams for this collection
        streamlock.acquire()
        newstreams = self._request_streams(colid, laststream)
        
        for s in newstreams:
            self.streams[s['stream_id']] = {'parser':parser, 'streaminfo':s,
                    'collection':colid}
           
            parser.add_stream(s)
            if s['stream_id'] > laststream:
                laststream = s['stream_id']
        streamlock.release()
        
        self.collection_lock.acquire()
        self.collections[colid]['laststream'] = laststream
        self.collections[colid]['lastchecked'] = now
        self.collection_lock.release()
           
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
        parser = self._lookup_parser(name)
        if parser == None:
            return []
        self._update_stream_map(name, parser)

        # This is all handled within the parser, as the parameters that can
        # be used as selection options will differ from collection to
        # collection.
        return parser.get_selection_options(params)

    def get_stream_info(self, name, streamid):
        """ Returns the stream information dictionary for a given stream.

            Parameters:
              name -- the collection that the stream belongs to
              streamid -- the id of the stream that the info is requested for
        """
        parser = self._lookup_parser(name)
        if parser == None:
            return {}

    
        colid, coldata = self._lookup_collection(name)
        if colid == None:
            return {}
        streamlock = coldata['streamlock']

        self._update_stream_map(name, parser)
        
        streamlock.acquire()
        if streamid not in self.streams:
            print "Failed to get stream info", streamid, self
            streamlock.release()
            return {}

        info = self.streams[streamid]['streaminfo']
        streamlock.release()
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
        parser = self._lookup_parser(name)
        if parser == None:
            return -1 

        self._update_stream_map(name, parser)
        return parser.get_stream_id(params)

    def get_collection_streams(self, collection):
        """ Returns a list of the streaminfo dicts for all streams belonging
            to the given collection.

            Parameters:
              collection -- the name of the collection to query

            Returns:
             a list of dictionaries where each dictionary contains the
             stream information for a stream belonging to the named collection.
             If the collection name is incorrect, an empty list is returned.
        """

        colstreams = []

        colid, coldata = self._lookup_collection(collection)
        if colid == None:
            return []
        parser = self._lookup_parser(collection)
        if parser == None:
            return [] 

        self._update_stream_map(collection, parser)

        streamlock = coldata['streamlock']
        streamlock.acquire()
        for s in self.streams.values():
            if s['collection'] == colid:
                colstreams.append(s['streaminfo'])

        streamlock.release()
        return colstreams

    def _query_related(self, collection, streaminfo):
        self.create_parser(collection)
        parser = self._lookup_parser(collection)
        if parser == None:
            return {}
       
        self._update_stream_map(collection, parser)
        return parser.get_graphtab_stream(streaminfo) 

    def _get_related_collections(self, colmodule):
        # We should already have a set of collections loaded by this point
        self.collection_lock.acquire()

        relatives = []

        for k,v in self.collections.items():
            if v['module'] == colmodule:
                relatives.append(v['name'])
        self.collection_lock.release()
        return relatives

    def get_related_streams(self, collection, streamid):
        colid, coldata = self._lookup_collection(collection)
        if colid == None:
            return {}
        
        parser = self._lookup_parser(collection)
        if parser == None:
            return {}
       
        self._update_stream_map(collection, parser)
        streamlock = coldata['streamlock']
         
        streamlock.acquire()
        if streamid not in self.streams:
            print "Failed to get stream info", streamid, self
            streamlock.release()
            return {}

        info = self.streams[streamid]['streaminfo']
        streamlock.release()

        relatedcols = self._get_related_collections(coldata['module'])

        result = {}
        for rel in relatedcols:
            relstreams = self._query_related(rel, info)
            for s in relstreams:
                result[s['title']] = s

        return result

    def _data_request_prep(self, collection, stream):
        """ Utility function that looks up the parser and collection ID 
            required for a _get_data call. Also checks if the stream is
            actually present in the collection.

            Returns a tuple (colid, parser) if everything was successful.
            Returns None if the collection doesn't exist, there is no parser
            for the collection or the stream doesn't exist in the collection.
        """
        colid, coldata = self._lookup_collection(collection)
        if colid == None:
            return None
        parser = self._lookup_parser(collection)
        if parser == None:
            return None
        self._update_stream_map(collection, parser)
        
        streamlock = coldata['streamlock']
         
        streamlock.acquire()
        if stream not in self.streams:
            print "Failed to find stream %s in collection %s" % \
                    (stream, collection)
            streamlock.release()
            return None
        streamlock.release()

        return colid, parser


    def get_recent_data(self, collection, stream, duration, detail):
        """ Returns aggregated data measurements for a time period starting 
            at 'now' and going back a specified number of seconds. This
            function is mainly useful for getting summary statistics for the
            last hour, day, week etc. 

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
              detail -- a string that describes the level of measurement detail
                        that should be returned for each datapoint. If None,
                        assumed to be "full".

            Returns:
              an ampy Result object containing all of the requested measurement
              data. If the request fails, this Result object will be empty.
        """
        
        check = self._data_request_prep(collection, stream)
        if check == None:
            return ampy.result.Result([])

        colid, parser = check
        
        if detail is None:
            detail = "full"

        end = int(time.time())
        start = end - duration


        # If we have memcache check if this data is available already.
        if self.memcache:
            
            query = {'stream': stream, 'duration':duration, 'detail':detail}
            cached = self.memcache.check_recent(query)

            if cached != []:
                return ampy.result.Result(cached)

        result, freq = self._get_data(colid, stream, start, end, duration, 
                detail, parser)

        result = parser.format_data(result, stream, 
                self.streams[stream]['streaminfo'])

        # Make sure we cache this result
        if self.memcache:
            self.memcache.store_recent(query, result)

        return ampy.result.Result(result)

    def get_period_data(self, collection, stream, start, end, binsize, detail):
        """ Returns data measurements for a time period explicitly described
            using a start and end time. The main purpose of this function is
            for querying for time series data that can be used to plot graphs.

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
              collection -- the name of the collection the stream belongs to
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
        check = self._data_request_prep(collection, stream)
        if check == None:
            return ampy.result.Result([])

        colid, parser = check
        
        # FIXME: Consider limiting maximum durations based on binsize
        # if end is not set then assume "now".
        if end is None:
            end = int(time.time())

        # If start is not set then assume 2 hours before the end.
        if start is None:
            start = end - (60 * 60 * 2)

        if detail is None:
            detail = "full"

        if self.memcache:
            datafreq = 0
            blocks = self.memcache.get_caching_blocks(stream, start, end, 
                    binsize, detail)
            required, cached = self.memcache.search_cached_blocks(blocks)
        
            queryresults = []
            for r in required:
                qr, freq = self._get_data(colid, stream, r['start'], 
                        r['end']-1, r['binsize'], detail, parser)
                queryresults += qr
                
                if datafreq == 0:
                    datafreq = freq

            return self._process_blocks(blocks, cached, queryresults, 
                    stream, parser, datafreq)     
       
        # Fallback option for cases where memcache isn't installed
        # XXX Should we just make memcache a requirement for ampy??
        data, freq = self._get_data(colid, stream, start, end, binsize, 
                detail, parser)

        if freq != 0 and len(data) != 0:
            data = self._fill_missing(data, freq, stream)

        # Some collections have some specific formatting they like to do to
        # the data before displaying it, e.g. rrd-smokeping combines the ping
        # data into a single list rather than being 20 separate dictionary
        # entries.

        data = parser.format_data(data, stream, self.streams[stream]['streaminfo'])
        return ampy.result.Result(data)

    def _process_blocks(self, blocks, cached, queried, stream, parser, freq):
        data = []
        now = int(time.time())

        for b in blocks:
        
            # Situations where the measurement frequency is greater than our
            # requested binsize are tricky. The returned values may not line
            # up nicely with the blocks that we are expecting, so we have to
            # do things a little differently
            if freq > b['binsize']:
                
                # Measurements are only going to be present at 'freq' intervals
                incrementby = freq

                # In this case, we should use the timestamp field. The
                # binstart field is calculated based on the requested binsize,
                # but whereas timestamp will correspond to the last, i.e. the
                # only, measurement included in the bin.
                usekey = 'timestamp'

                # Our block start and end values have been calculated based on
                # the requested binsize. These won't line up nicely with the 
                # bins present in queried, so we need to adjust our start and
                # end times to make sure every measurement ends up in the
                # right block
                if (b['start'] % freq) != 0:
                    ts = b['start'] + (freq - (b['start'] % freq))
                else:
                    ts = b['start']
            else:
                # This is the general case where the binsize we requested
                # is greater than or equal to the measurement frequency

                # We don't need to try and hax our blocks at all
                incrementby = b['binsize']
                ts = b['start']
                end = b['end']
                usekey = 'binstart'

            # If this block was cached, just chuck the cached data into our
            # result and move on to the next block
            if b['start'] in cached:
                data += cached[b['start']]
                continue

            blockdata = []
        

            while ts < b['end']:
                if ts > now:
                    break
                
                if len(queried) > 0 and \
                        int(queried[0][usekey]) - ts < incrementby:
                    datum = queried[0]
                    queried = queried[1:]
                else:
                    datum = {"binstart":ts, "timestamp":ts, "stream_id":stream}

                blockdata.append(datum)
                ts += incrementby

            blockdata = parser.format_data(blockdata, stream,
                    self.streams[stream]['streaminfo'])
   
            if blockdata != []:
                data += blockdata
                # Got all the data for this uncached block -- cache it
                self.memcache.store_block(b, blockdata)

        return ampy.result.Result(data) 

    def _fill_missing(self, data, freq, stream):
        """ Internal function that populates the data list with 'empty'
            measurements wherever a data point is missing. This will ensure
            that amp-web will break line graphs wherever data is missing.
        """
        nextts = data[0]['binstart']
        nogap_data = []

        # If there are missing measurements, make sure we create 'None'
        # entries for them so that our graphs are discontinuous. If we don't
        # do this, then there'll be a hideous straight line linking the
        # data points either side of the gap
        for d in data:

            while d['binstart'] - nextts >= freq:
                nogap_data.append({'binstart':nextts, 'stream_id':stream, 
                        'timestamp':nextts})
                nextts += freq
            nogap_data.append(d)
            nextts = d['binstart'] + freq

        return nogap_data


    def _get_data(self, colid, stream, start, end, binsize, detail, parser):
        """ Internal function that actually performs the NNTSC query to get
            measurement data, parses the responses and formats the results
            appropriately.

            Returns a list of formatted results that can be used to create
            an ampy Result object.

            Parameters:
                the same as get_period_data()

            Returns:
                a list of formatted results that can be used to create
                an ampy Result object.
        """
        if parser == None:
            print >> sys.stderr, "Cannot fetch data -- no valid parser for stream %s" % (stream)
            return ampy.result.Result([])


        client = self._connect_nntsc()
        if client == None:
            print >> sys.stderr, "Cannot fetch data -- lost connection to NNTSC"
            return ampy.result.Result([])

        result = parser.request_data(client, colid, stream, start, end, 
                binsize, detail)

        if result == -1:
            client.disconnect()
            return ampy.result.Result([])

        got_data = False
        data = []
        freq = 0

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
                continue

            if msg[0] == NNTSC_HISTORY:
                # Sanity checks
                if msg[1]['collection'] != colid:
                    continue
                if msg[1]['streamid'] != stream:
                    continue
                #if msg[1]['aggregator'] != agg_functions:
                #   continue

                freq = msg[1]['binsize']
                data += msg[1]['data']
                if msg[1]['more'] == False:
                    got_data = True
        client.disconnect()

        return data, freq


# vim: set smartindent shiftwidth=4 tabstop=4 softtabstop=4 expandtab :
