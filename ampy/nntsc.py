#!/usr/bin/env python

import time
import sys
import re

import ampy.result

import socket
from libnntscclient.protocol import *
from libnntscclient.nntscclient import NNTSCClient

from ampy.muninbytes import MuninbytesParser
from ampy.lpibytes import LPIBytesParser
from ampy.smokeping import SmokepingParser
from ampy.lpipackets import LPIPacketsParser
from ampy.lpiflows import LPIFlowsParser
from ampy.lpiusers import LPIUsersParser
from ampy.ampicmp import AmpIcmpParser
from ampy.amptraceroute import AmpTracerouteParser
from ampy.ampdns import AmpDnsParser
from ampy.views import View


from threading import Lock

try:
    import pylibmc
    from ampy.caching import AmpyCache
    _have_memcache = True
except ImportError:
    _have_memcache = False

STREAM_CHECK_FREQUENCY = 60 * 5
ACTIVE_STREAM_CHECK_FREQUENCY = 60 * 30

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
        else:
            self.ampdbconfig['host'] = None
            self.ampdbconfig['user'] = None
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

        # Set up access to the views that will convert view_ids to stream_ids
        # TODO use it's own config, not the ampdbconfig
        self.view = View(self, self.ampdbconfig)

        # Keep track of (roughly) when each stream id has been active so
        # that we can cull the search space slightly. If we know an id hasn't
        # been active for a month then there is no point in checking if it has
        # data in the last day.
        self.activity = {}
        self.activity_lock = Lock()


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
            print >> sys.stderr, "Failed to connect to %s:%d -- %s" % (
                    self.host, self.port, msg[1])
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

    def _request_stream_activity(self, colid, lastactivity):
        """ Query NNTSC for all of the streams for a given collection

            Parameters:
              colid -- the id number of the collection to query for
              lastactivity -- the last time activity was checked for

            Returns:
              a list of streams
        """
        streams = []
        client = self._connect_nntsc()

        if client == None:
            print >> sys.stderr, "Unable to connect to NNTSC exporter to request streams"
            return []

        client.send_request(NNTSC_REQ_ACTIVE_STREAMS, colid, lastactivity)
        while 1:
            msg = self._get_nntsc_message(client)
            if msg == None:
                client.disconnect()
                return []

            # Check if we got a complete parsed message, otherwise read some
            # more data
            if msg[0] == -1:
                continue
            if msg[0] != NNTSC_ACTIVE_STREAMS:
                print >> sys.stderr, "Expected NNTSC_ACTIVE_STREAMS response, not %d" % (msg[0])
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
            self.collections[col['id']] = {'name':name, 'label':label,
                    'laststream':0, 'lastchecked':0, 'streamlock':Lock(),
                    'module':col['module'], 'streams':{}, 'lastactivity':0}
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

        return self.collections

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

        if name == "amp-dns":
            parser = AmpDnsParser(self.ampdbconfig)

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

    def _update_stream_activity(self, colid, coldata, parser):
        # Make sure only one thread will update the activity times at once.
        # TODO could we get away with a non-blocking attempt to acquire the
        # lock, and just carry on about our business if we can't get it?
        self.activity_lock.acquire()

        data = []
        fetched = False
        now = time.time()
        lastactivity = coldata["lastactivity"]

        # try to fetch memcached data first, if available
        if self.memcache:
            data = self.memcache.check_active_streams(colid)

        # if we don't have memcache or it has expired, fetch new data
        if len(data) == 0:
            data = self._request_stream_activity(colid, lastactivity)
            # only update last activity if we have fetched new data
            self.collection_lock.acquire()
            coldata["lastactivity"] = now
            self.collection_lock.release()
            fetched = True

        # update all the streams that we get in the response
        for stream in data:
            if stream not in self.activity:
                # this will get updated properly once we receive the streaminfo
                self.activity[stream] = {"first": now, "last": now}
            else:
                self.activity[stream]["last"] = now

        # try to cache it if we got fresh data
        if self.memcache and fetched and len(data) > 0:
            self.memcache.store_active_streams(colid, data)
        self.activity_lock.release()


    def _update_streams(self, colid, coldata, parser):
        data = []
        fetched = False
        maxts = 0
        lastactivity = coldata['lastactivity']
        laststream = coldata['laststream']
        streamlock = coldata['streamlock']
        streams = coldata['streams']

        streamlock.acquire()
        self.activity_lock.acquire()

        # Try to fetch this data from the cache first.
        if self.memcache:
            # This is just a list of stream ids, with no stream info
            data = self.memcache.check_collection_streams(colid)

        # If it isn't cached or we don't have a cache, then fetch it properly,
        # with full stream info and everything. If laststream is zero then we
        # will get every stream, otherwise we will only get new streams since
        # we last got checked.
        if len(data) == 0:
            data = self._request_streams(colid, laststream)
            fetched = True

        if not fetched:
            # We might have already seen some (if not all) of it.
            existing = set(streams.keys())

            # Trim the data down to just the streams that are in the cache
            # that we don't already know about.
            data = data.difference(existing).union(existing.difference(data))

        for s in data:
            # Cache the individual stream info if we can do caching
            if self.memcache:
                if fetched:
                    # Save this stream information that has just been fetched
                    self.memcache.store_streaminfo(s, s['stream_id'])
                else:
                    # Lookup the stream id, the info should be cached
                    s = self.memcache.check_streaminfo(s)
                    if len(s) == 0:
                        s = self.get_stream_info(colid, s)
                        if len(s) == 0:
                            continue

            # Save the stream info in our local store
            streams[s['stream_id']] = {
                'parser': parser,
                'streaminfo': s,
                'collection': colid
            }

            # Add the stream to the parser specific to the collection
            parser.add_stream(s)

            # Fill in the initial times that this stream was active
            self.activity[s['stream_id']] = {
                'first': s['firsttimestamp'],
                'last': s['lasttimestamp']
            }

            # Make sure we track the most recent stream id we have seen
            if s['stream_id'] > laststream:
                laststream = s['stream_id']
            # Track the most recent time any stream was active
            if s['lasttimestamp'] > maxts:
                maxts = s['lasttimestamp']

        # Cache the list of all streams if possible
        if self.memcache:
            self.memcache.store_collection_streams(colid, set(streams.keys()))

        # Release the locks, other threads can now try to get data and will
        # hopefully hit cached versions.
        self.activity_lock.release()
        streamlock.release()

        # Update the last checked timestamps and last stream
        self.collection_lock.acquire()
        self.collections[colid]['laststream'] = laststream
        self.collections[colid]['lastchecked'] = time.time()
        if lastactivity == 0:
            self.collections[colid]['lastactivity'] = maxts
        self.collection_lock.release()


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

        lastchecked = coldata['lastchecked']

        # Avoid requesting new stream information if we have done so recently
        now = time.time()

        if now < (lastchecked + STREAM_CHECK_FREQUENCY):
            return
        self._update_streams(colid, coldata, parser)

        # Check this less frequently, we don't need to be too precise and
        # can save time and effort by sending less data less frequently. Also,
        # this will be skipped the first time as the initial stream activity
        # information can be gained through the full stream info that we just
        # fetched immediately above
        lastactivity = coldata['lastactivity']
        if now > (lastactivity + ACTIVE_STREAM_CHECK_FREQUENCY):
            # update last activity time for streams for this collection
            self._update_stream_activity(colid, coldata, parser)



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
    
    def get_graphtab_group(self, name, splitrule):
        parser = self._lookup_parser(name)
        if parser == None:
            return None
       
        self._update_stream_map(name, parser)
        
        newrule = parser.get_graphtab_group(splitrule)
        return newrule

    def event_to_group(self, name, streamid):
        info = self.get_stream_info(name, streamid)
        if info == {}:
            return ""

        parser = self._lookup_parser(name)
        if parser == None:
            return {}

        return parser.event_to_group(info)

    def stream_to_group(self, name, streamid):
        info = self.get_stream_info(name, streamid)
        if info == {}:
            return ""

        parser = self._lookup_parser(name)
        if parser == None:
            return {}

        return parser.stream_to_group(info)

    def parse_group_options(self, name, options):
        parser = self._lookup_parser(name)
        if parser == None:
            return ""

        return parser.parse_group_options(options)

    def split_group_rule(self, name, rule):
        parser = self._lookup_parser(name)
        if parser == None:
            return {}
       
        parts, keydict = parser.split_group_rule(rule)
        return parts

    def find_group_streams(self, name, rule, groupid):
        parser = self._lookup_parser(name)
        if parser == None:
            return {}

        parts, keydict = parser.split_group_rule(rule)
        groupstreams = self.get_stream_id(name, keydict)

        if type(groupstreams) != list or len(groupstreams) == 0:
            return {}

        # Not every group will require stream info, but it is easier
        # to get it now than have to try and get it once we're inside
        # the parser
        groupinfo = {}
        for s in groupstreams:
            groupinfo[s] = self.get_stream_info(name, s)

        return parser.find_groups(parts, groupinfo, groupid)


    def get_view_legend(self, name, viewid):
        parser = self._lookup_parser(name)
        if parser == None:
            return {}

        viewgroups = self.view.get_view_groups(name, viewid)

        legend = {}

        sortedgids = viewgroups.keys()
        sortedgids.sort()

        seriesid = 0
        for gid in sortedgids:
            rule = viewgroups[gid]
            legend[gid] = {}
            legend[gid]['label'] = parser.legend_label(rule)
            groupstreams = self.find_group_streams(name, rule, gid);

            morekeys = groupstreams.keys()
            morekeys.sort()

            legend[gid]['keys'] = []
            for k in morekeys:
                linelabel = parser.line_label(groupstreams[k])
                legend[gid]['keys'].append((k, linelabel, seriesid))
                seriesid += 1

        return legend


    def get_stream_info(self, name, streamid):
        """ Returns the stream information dictionary for a given stream.

            Parameters:
              name -- the collection that the stream belongs to
              streamid -- the id of the stream that the info is requested for
        """
        # If we have a memcache and this stream is in there, just grab the
        # stream info straight out of there
        if self.memcache:
            info = self.memcache.check_streaminfo(streamid);

            if info != {}:
                return info

        # Otherwise, we'll have to do this the old-fashioned way

        parser = self._lookup_parser(name)
        if parser == None:
            return {}

        colid, coldata = self._lookup_collection(name)
        if colid == None:
            return {}
        streamlock = coldata['streamlock']
        streams = coldata['streams']

        self._update_stream_map(name, parser)

        streamlock.acquire()
        if streamid not in streams:
            print "Failed to get stream info", streamid, coldata
            streamlock.release()
            return {}

        info = streams[streamid]['streaminfo']
        streamlock.release()
        return info

    def get_stream_id(self, name, params):
        """ Finds the ID of the streams that match the provided parameters.

            To be successful, the params dictionary must contain most if not
            all of the possible selection parameters for the collection. For
            example, a rrd-muninbytes stream ID will only be found if the
            params dictionary contains 'switch', 'interface' AND 'direction'.

            See also get_selection_options().

            Parameters:
              name -- the name of the collection to search for the stream
              params -- a dictionary containing parameters describing the
                        stream that is being searched for. All possible
                        identifying parameters must be present to find the
                        stream.

            Returns:
              a list of id numbers for the streams that match the given
              parameters. An empty list is returned if a unique match was not
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
        streams = coldata['streams']
        streamlock.acquire()
        for s in streams.values():
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

    def get_related_streams(self, collection, streamids):
        colid, coldata = self._lookup_collection(collection)
        if colid == None:
            return {}

        parser = self._lookup_parser(collection)
        if parser == None:
            return {}

        self._update_stream_map(collection, parser)
        streamlock = coldata['streamlock']
        streams = coldata['streams']
        relatedcols = self._get_related_collections(coldata['module'])
        streamlock.acquire()

        result = {}

        for i in streamids:
            sid = int(i)

            if sid not in streams:
                print "Failed to get stream info", sid
                continue
            info = streams[sid]['streaminfo']

            for rel in relatedcols:
                relstreams = self._query_related(rel, info)
                for s in relstreams:
                    title = s['title']
                    col = s['collection']
                    relid = s['streamid']

                    if title in result:
                        result[title]['streamid'][i] = relid
                    else:
                        result[title] = {}
                        result[title]['collection'] = col
                        result[title]['title'] = title
                        result[title]['streamid'] = {i: relid}

        streamlock.release()

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
        streams = coldata['streams']
        streamlock.acquire()
        if stream not in streams:
            print "Failed to find stream %s in collection %s" % \
                    (stream, collection)
            streamlock.release()
            return None
        streaminfo = streams[stream]['streaminfo']
        streamlock.release()

        return colid, parser, streaminfo


    def _filter_active_streams(self, labels, start, end):
        # Fudge the cutoff slightly to exclude anything inactive since
        # twice the cache duration for activity before the start time.
        cutoff = start - (ACTIVE_STREAM_CHECK_FREQUENCY * 2)
        print "LABELS:", len(labels)

        count = 0
        for label, streams in labels.iteritems():
            count += len(streams)
        print "STREAMS IN LABELS:", count

        count = 0
        # use labels.items() to operate on a copy, so that we can delete empty
        # stream lists from the original
        for label, streams in labels.items():
            active = [s for s in streams \
                     if self.activity[s]["last"] > cutoff and \
                        self.activity[s]["first"] < end]
            if len(active) > 0:
                labels[label] = active
            else:
                del labels[label]
            count += len(active)

        # TODO remove debug
        print "ACTIVE STREAMS IN LABELS:", count
        print "start:", start, "end:", end, "cutoff:", cutoff
        return labels


    def get_recent_view_data(self, collection, view_id, duration, detail):
        """ Returns aggregated data measurements for a time period starting
            at 'now' and going back a specified number of seconds. This
            function is mainly useful for getting summary statistics for the
            last hour, day, week etc.

            See also get_period_view_data().

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
              view_id -- the id number of the view to fetch data for
              duration -- the length of the time period to fetch data for, in
                          seconds
              detail -- a string that describes the level of measurement detail
                        that should be returned for each datapoint. If None,
                        assumed to be "full".

            Returns:
              a dictionary containing all of the requested measurement data,
              indexed by view_id. If the request fails, this will be empty.
        """

        info = {}
        data = {}
        queries = {}
        if detail is None:
            detail = "full"
        end = int(time.time())
        start = end - duration

        # figure out what lines should be displayed in this view
        labels = self.view.get_view_streams(collection, view_id)
        print "INITIAL LABELS:", len(labels)
        count = 0
        for label, streams in labels.iteritems():
            count += len(streams)
        print "INITIAL STREAMS IN LABELS:", count

        # determine which of these stream ids were possibly active in the
        # last given time period
        labels = self._filter_active_streams(labels, start, end)

        if len(labels.keys()) == 0:
            return {}

        # TODO pick a stream id? these should all be the same collection etc
        # maybe we do need to call this on every stream_id to be sure?
        collection_id, parser, streaminfo = self._data_request_prep(
                collection, labels.values()[0][0])

        # check each stream_id to see if we need to query for it - some will
        # be invalid, some will be cached already, so don't fetch those ones
        for label, streams in labels.iteritems():
            # an invalid or unknown stream_id has empty data, don't query it
            #if stream_id > 0:
            #    info[stream_id] = self._data_request_prep(collection, stream_id)
            #if stream_id < 0 or info[stream_id] == None:
            #    data[stream_id] = []
            #    continue

            # If we have memcache check if this data is available already.
            if self.memcache:
                key = {
                    'label': label,
                    'duration': duration,
                    'detail': detail
                }
                cached = self.memcache.check_recent(key)

                if cached != None:
                    data[label] = cached
                    continue
            # Otherwise, we don't have it already so add to the list to query
            queries[label] = labels[label]

        # if there are any labels that we don't already have data for, now
        # is the time to fetch them and cache them for later
        if len(queries) > 0:
            # Fetch all the data that we don't already have. All these streams
            # are in the same collection and therefore use the same parser, so
            # we can just grab the collection_id and parser from the last
            # stream_id we touched.
            #collection_id,parser,_ = info[queries[0]]
            result = self._get_data(collection_id, queries, start, end,
                    duration, detail, parser)

            # do any special formatting that the parser requires
            for label in result.keys():
                #_,_,streaminfo = info[stream_id]
                result[label] = parser.format_data(result[label],
                        label, streaminfo)

            # Make sure we cache any results we just fetched
            if self.memcache:
                for label in result.keys():
                    key = {
                        'label': label,
                        'duration': duration,
                        'detail': detail
                    }
                    self.memcache.store_recent(key, result[label]["data"])

            # we only store the data portion of the result (not freq etc),
            # so merge that with cached data
            for k, v in result.iteritems():
                data[k] = v["data"]
        return data

    def get_period_view_data(self, collection, view_id, start, end, binsize, detail):
        blocks = {}
        cached = {}
        data = {}
        # FIXME: Consider limiting maximum durations based on binsize
        # if end is not set then assume "now".
        if end is None:
            end = int(time.time())
        # If start is not set then assume 2 hours before the end.
        if start is None:
            start = end - (60 * 60 * 2)
        if detail is None:
            detail = "full"

        # figure out what lines should be displayed in this view
        labels = self.view.get_view_streams(collection, view_id)
        labels = self._filter_active_streams(labels, start, end)

        if len(labels.keys()) == 0:
            return {}

        # TODO pick a stream id? these should all be the same collection etc
        # maybe we do need to call this on every stream_id to be sure?
        collection_id, parser, streaminfo = self._data_request_prep(
                collection, labels.values()[0][0])

        if self.memcache:
            # see if any of them have been cached
            required = {}
            for label, streams in labels.iteritems():
                # treat a label as an entity that we cache - it might be made
                # up of data from lots of streams, but it's only one set of data
                blocks[label] = self.memcache.get_caching_blocks(label,
                        start, end, binsize, detail)
                req, cached[label] = self.memcache.search_cached_blocks(
                        blocks[label])
                #print "cached:%d uncached:%d id:%s start:%d end:%d" % (
                #        len(cached[label]), len(req), label, start, end)
                for r in req:
                    # group up all the labels that need the same time blocks
                    blockstart = r["start"]
                    blockend = r["end"]
                    binsize = r["binsize"]
                    if blockstart not in required:
                        required[blockstart] = {}
                    if blockend not in required[blockstart]:
                        required[blockstart][blockend] = {}
                    if binsize not in required[blockstart][blockend]:
                        required[blockstart][blockend][binsize] = {}
                    required[blockstart][blockend][binsize][label] = streams

            # fetch those that aren't cached
            fetched = {}
            frequencies = {}
            for bstart in required:
                for bend in required[bstart]:
                    for binsize, rqlabels in required[bstart][bend].iteritems():
                        qr = self._get_data(collection_id, rqlabels, bstart,
                                bend-1, binsize, detail, parser)
                        #print qr
                        if len(qr) == 0:
                            return None
                        for label, item in qr.iteritems():
                            if label not in fetched:
                                fetched[label] = []
                            fetched[label] += item["data"]
                            frequencies[label] = item["freq"]

            # deal with all the labels that we have fetched data for just now
            for label, item in fetched.iteritems():
                data[label] = self._process_blocks(
                        blocks[label], cached[label],
                        fetched[label], label, streaminfo,
                        parser, frequencies[label])
            # deal with any streams that were entirely cached - should be
            # every stream left that hasn't already been touched
            for label, item in cached.iteritems():
                if label not in fetched:
                    data[label] = self._process_blocks(
                            blocks[label], cached[label],
                            [], label, streaminfo, parser, 0)
            return data


    def _process_blocks(self, blocks, cached, queried, stream, streaminfo,
            parser, freq):
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

            seendata = False
            first = True

            while ts < b['end']:
                if ts > now:
                    break

                if len(queried) > 0 and \
                        int(queried[0][usekey]) - ts < incrementby:
                    datum = queried[0]
                    queried = queried[1:]
                    seendata = True
                else:
                    datum = {"binstart":ts, "timestamp":ts, "stream_id":stream}

                # always add the first datum, regardless of whether it is good
                # data or not - if it's not good then we want to add a null
                # value to ensure we don't draw a line from the previous block
                if seendata or first:
                    first = False
                    blockdata.append(datum)
                ts += incrementby

            blockdata = parser.format_data(blockdata, stream, streaminfo)

            # XXX try storing all data, even if empty - we can cache negative
            # data too!
            #if blockdata != []:
            data += blockdata
            # Got all the data for this uncached block -- cache it
            self.memcache.store_block(b, blockdata)

        return data

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


    def _get_data(self, colid, labels, start, end, binsize, detail, parser):
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
            print >> sys.stderr, "Cannot fetch data -- no valid parser for stream %s" % (labels)
            return {}

        client = self._connect_nntsc()
        if client == None:
            print >> sys.stderr, "Cannot fetch data -- lost connection to NNTSC"
            return {}

        #print "requesting data for streams, detail", detail, stream_ids
        result = parser.request_data(client, colid, labels, start, end,
                binsize, detail)

        if result == -1:
            client.disconnect()
            return {}

        data = {}
        freq = 0
        count = 0

        while count < len(labels):
            #print "got message %d/%d" % (count+1, len(labels))
            msg = self._get_nntsc_message(client)
            #print msg
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
                label = msg[1]['streamid']
                # XXX extra checks for old streamid data
                if label not in labels and int(label) not in labels:
                    continue
                #if msg[1]['aggregator'] != agg_functions:
                #   continue
                if label not in data:
                    data[label] = {}
                    data[label]["data"] = []
                    data[label]["freq"] = msg[1]['binsize']
                data[label]["data"] += msg[1]['data']
                if msg[1]['more'] == False:
                    # increment the count of completed labels
                    count += 1
        #print "got all messages"
        client.disconnect()
        return data


# vim: set smartindent shiftwidth=4 tabstop=4 softtabstop=4 expandtab :
