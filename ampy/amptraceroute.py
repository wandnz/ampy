#!/usr/bin/env python

import sys, string
import amp

class AmpTracerouteParser(amp.AmpParser):
    """ Parser for the amp-traceroute collection. """

    def __init__(self):
        """ Initialises the parser """
        # we need all the same initialisation everything AMP related does
        super(AmpTracerouteParser, self).__init__()

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
        return self.streams[key]

    def get_aggregate_functions(self, detail):
        """ Return the aggregation functions that should be applied to the
            columns returned by get_aggregate_columns(). It should either be
            a list of the same length, describing the aggregation function to
            use for each column, or it should be a string describing the
            function to use across all columns """
        return "avg"

    def get_aggregate_columns(self, detail):
        """ Return a list of columns in the data table for this collection
            that should be subject to data aggregation """
        return ["length"]

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

            If '_requesting' is set to 'packet_sizes' and 'source' is set and
            'destination' is set, this will return the list of packet sizes
            used in tests between that source and that destination. If either
            of 'source' or 'destination' is not set, then a list of all
            packet sizes across all streams is returned.
        """
        # let the parent take care of this if it is an operation common to
        # all AMP tests: fetching sites, meshes, etc
        result = super(AmpTracerouteParser, self).get_selection_options(params)
        if len(result) > 0:
            return result

        # otherwise check for the options specific to this type
        if params["_requesting"] == "packet_sizes":
            if 'source' not in params or 'destination' not in params:
                return []
            else:
                return self._get_sizes(params['source'], params['destination'])
        return []

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
