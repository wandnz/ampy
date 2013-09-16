#!/usr/bin/env python

import sys, string
import amp

class AmpIcmpParser(amp.AmpParser):
    """ Parser for the amp-icmp collection. """

    def __init__(self, dbconfig):
        """ Initialises the parser """
        # we need all the same initialisation everything AMP related does
        super(AmpIcmpParser, self).__init__(dbconfig)

        # Dictionary that maps (source, dest) to a set of packet sizes that
        # have been used to test between those two hosts
        self.sizes = {}

    # XXX do we want to extract the source/destination parts of this function
    # into the parent class?
    def add_stream(self, s):
        """ Updates the internal maps based on a new stream

            Parameters:
                s -- the new stream, as returned by NNTSC
        """
        if s['destination'] in self.sources:
            self.sources[s['destination']][s['source']] = 1
        else:
            self.sources[s['destination']] = {s['source']:1}

        if s['source'] in self.destinations:
            self.destinations[s['source']][s['destination']] = 1
        else:
            self.destinations[s['source']] = {s['destination']:1}

        if (s['source'], s['destination']) in self.sizes:
            self.sizes[(s['source'], s['destination'])][s['packet_size']] = 1
        else:
            self.sizes[(s['source'], s['destination'])] = {s['packet_size']:1}

        self.streams[(s['source'], s['destination'], s['packet_size'])] = s['stream_id']

    def get_stream_id(self, params):
        """ Finds the stream ID that matches the given (source, dest, size)
            combination.

            If params does not contain an entry for 'source', 'destination',
            or 'packet_size', then -1 will be returned.

            Parameters:
                params -- a dictionary containing the parameters describing the
                          stream to search for

            Returns:
                the id number of the matching stream, or -1 if no matching
                stream can be found
        """

        if 'source' not in params:
            return -1
        if 'destination' not in params:
            return -1
        if 'packet_size' not in params:
            return -1

        key = (params['source'], params['destination'], params['packet_size'])
        if key not in self.streams:
            return -1
        return [self.streams[key]]

    def request_data(self, client, colid, streams, start, end, binsize, detail):
        """ Based on the level of detail requested, forms and sends a request
            to NNTSC for aggregated data.
        """

        # the matrix view expects both the mean and stddev for the latency
        if detail == "matrix":
            aggfuncs = ["avg", "stddev", "avg"]
            aggcols = ["rtt", "rtt", "loss"]
        else:
            aggfuncs = ["avg", "avg"]
            aggcols = ["rtt", "loss"]

        # 'full' implies a smokeping-style graph, so we'll need to grab
        # the percentile data
        if detail == "full":
            result = client.request_percentiles(colid, streams, start, end,
                    aggcols, binsize, ["stream_id"], aggfuncs)
        else:
            result = client.request_aggregate(colid, streams, start, end,
                    aggcols, binsize, ["stream_id"], aggfuncs)
        return result

    def format_data(self, received, stream, streaminfo):
        """ Formats the measurements retrieved from NNTSC into a nice format
            for subsequent analysis / plotting / etc.

        """
        # Don't do anything to this data, it is probably fine as is
        return received

    def get_selection_options(self, params):
        """ Returns the list of names to populate a dropdown list with, given
            a current set of selected parameters.

            If '_requesting' is set to 'packet_sizes' and 'source' is set and
            'destination' is set, this will return the list of packet sizes
            used in tests between that source and that destination. If either
            of 'source' or 'destination' is not set, then a list of all
            packet sizes across all streams is returned.
        """
        # let the parent take care of this if it is an operation common to
        # all AMP tests: fetching sites, meshes, etc
        result = super(AmpIcmpParser, self).get_selection_options(params)
        if len(result) > 0:
            return result

        # otherwise check for the options specific to this type
        if params["_requesting"] == "packet_sizes":
            if 'source' not in params or 'destination' not in params:
                return []
            else:
                return self._get_sizes(params['source'], params['destination'])
        return []

    def get_graphtab_stream(self, streaminfo):
        """ Given the description of a stream from a similar collection,
            return the stream id of the stream from this collection that is
            suitable for display on a graphtab alongside the main graph for
            the provided stream.
        """

        if 'source' not in streaminfo or 'destination' not in streaminfo:
            return []

        sizes = self._get_sizes(streaminfo['source'],
                streaminfo['destination'])

        if sizes == []:
            return []

        params = {'source':streaminfo['source'],
                'destination':streaminfo['destination']}

        # First, try to find a packet size that matches the packet size of
        # the original stream.
        # If that fails, try to use a 84 byte packet size (as this is the
        # default for icmp tests)
        # If that fails, pick the smallest size available

        if 'packet_size' in streaminfo and streaminfo['packet_size'] in sizes:
            params['packet_size'] = streaminfo['packet_size']
        elif '84' in sizes:
            params['packet_size'] = '84'
        else:
            sizes.sort(key=int)
            params['packet_size'] = sizes[0]

        return [{'streamid':self.get_stream_id(params), 'title':"Latency", \
                'collection':'amp-icmp'}]


    def _get_sizes(self, source, dest):
        """ Get a list of all packet sizes used to test between a given
            source and destination. If either source or dest is None, return
            all packet sizes that have been used across all streams.
        """
        if source != None and dest != None:
            if (source, dest) not in self.sizes:
                return []
            else:
                return self.sizes[(source, dest)].keys()
        sizes = {}
        for v in self.sizes.values():
            for d in v.keys():
                sizes[d] = 1
        return sizes.keys()

# vim: set smartindent shiftwidth=4 tabstop=4 softtabstop=4 expandtab :
