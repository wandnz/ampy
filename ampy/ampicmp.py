#!/usr/bin/env python

import sys, string

class AmpIcmpParser(object):
    """ Parser for the amp-icmp collection. """

    def __init__(self):
        """ Initialises the parser """

        # These dictionaries store local copies of data that relate to 
        # identifying streams, so we don't have to query the database
        # everytime someone selects a new 'source' on the graph page.
        #
        # In current amp-web, we want a set for each dropdown you implement
        # plus a 'streams' map so that you can look up the stream id once all
        # of the selection options have been set.


        # Dictionary that maps (dest) to a set of sources that test to that
        # destination. Allows us to look up sources based on a given 
        # destination
        self.sources = {}

        # Dictionary that maps (source) to a set of destinations for that source
        self.destinations = {}

        # Dictionary that maps (source, dest) to a set of packet sizes that 
        # have been used to test between those two hosts
        self.sizes = {}

        # Dictionary that maps (source, dest, size) to the corresponding
        # stream id
        self.streams = {}

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
        return self.streams[key]

    def get_aggregate_columns(self, detail):
        """ Return a list of columns in the data table for this collection
            that should be subject to data aggregation """

        return ["rtt", "loss"]

    def get_group_columns(self):
        """ Return a list of columns in the streams table that should be used
            to group aggregated data """
        return ["stream_id"]

    def format_data(self, received, stream, streaminfo):
        """ Formats the measurements retrieved from NNTSC into a nice format
            for subsequent analysis / plotting / etc.
    
        """
           
        # Don't do anything to this data, it is probably fine as is 
        return received

    def get_selection_options(self, params):
        """ Returns the list of names to populate a dropdown list with, given
            a current set of selected parameters.
        
            The '_requesting' parameter must be set to describe which dropdown
            you are trying to populate. If not set, this function will return
            an empty list.

            If '_requesting' is set to 'sources' and 'destination' is set,
            this will return a list of sources that test to the given dest.
            If 'destination' is not set, then all known sources are returned.

            If '_requesting' is set to 'destinations' and 'source' is set,
            this will return the list of destinations tested to by that source.
            If 'source' is not set, then all possible destinations will be
            returned.

            If '_requesting' is set to 'packet_sizes' and 'source' is set and
            'destination' is set, this will return the list of packet sizes
            used in tests between that source and that destination. If either
            of 'source' or 'destination' is not set, then a list of all
            packet sizes across all streams is returned.
        """
            
        if "_requesting" not in params:
            return []

        if params["_requesting"] == 'sources':
            if 'destination' in params:
                return self._get_sources(params['destination'])
            else:
                return self._get_sources(None)

        if params["_requesting"] == "destinations":
            if 'source' in params:
                return self._get_destinations(params['source'])
            else:
                return self._get_destinations(None)

        if params["_requesting"] == "packet_sizes":
            if 'source' not in params or 'destination' not in params:
                return []
            else:
                return self._get_sizes(params['source'], params['destination'])

        return []

    def _get_sources(self, dest):
        """ Get a list of all sources that test to a given destination. 
            If the destination is None, return all known sources.
        """
        if dest != None:
            if dest not in self.sources:
                return []
            else:
                return self.sources[dest].keys()

        srcs = {}
        for v in self.sources.values():
            for d in v.keys():
                srcs[d] = 1
        return srcs.keys()

    def _get_destinations(self, source):
        """ Get a list of all destinations that are tested to by a given
            source. If the source is None, return all possible destinations.
        """
        if source != None:
            if source not in self.destinations:
                return []
            else:
                return self.destinations[source].keys()

        dests = {}
        for v in self.destinations.values():
            for d in v.keys():
                dests[d] = 1
        return dests.keys()

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
