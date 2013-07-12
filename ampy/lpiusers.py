#!/usr/bin/env python

import sys, string

class LPIUsersParser(object):
    """ Parser for the lpi-metrics collection. """
    def __init__(self):
        """ Initialises the parser """
        self.streams = {}

        # Map containing all the valid sources
        self.sources = {}
        # Map containing all the valid protocols
        self.protocols = {}
        # Map containing all the valid metrics
        self.metrics = {}

    def add_stream(self, s):
        """ Updates the internal maps based on a new stream 

            Parameters:
              s -- the new stream, as returned by NNTSC
        """

        s['protocol'] = string.replace(s['protocol'], "/", " - ")

        self.sources[s['source']] = 1
        self.protocols[s['protocol']] = 1
        self.metrics[s['metric']] = 1

        self.streams[(s['source'], s['protocol'], s['metric'])] = s['stream_id']

    def get_stream_id(self, params):
        """ Finds the stream ID that matches the given (source, protocol,
            metric) combination.

            If params does not contain an entry for 'source', 'protocol'
            or 'metric', then -1 will be returned.

            Parameters:
                params -- a dictionary containing the parameters describing the
                          stream to search for

            Returns:
                the id number of the matching stream, or -1 if no matching
                stream can be found
        """

        if 'source' not in params:
            return -1;
        if 'protocol' not in params:
            return -1;
        if 'metric' not in params:
            return -1

        key = (params['source'], params['protocol'], params['metric'])
        if key not in self.streams:
            return -1
        return self.streams[key]

    def get_aggregate_columns(self, detail):
        """ Return a list of columns in the data table for this collection
            that should be subject to data aggregation """

        return ['users']

    def get_group_columns(self):
        """ Return a list of columns in the streams table that should be used
            to group aggregated data """

        return ['stream_id']

    def format_data(self, received, stream, streaminfo):
        """ Formats the measurements retrieved from NNTSC into a nice format
            for subsequent analysis / plotting / etc.

            In the case of lpi-users, no modification should be necessary.
        """

        return received


    def get_selection_options(self, params):
        """ Returns the list of names to populate a dropdown list with, given
            a current set of selected parameters.

            params must have a field called "_requesting" which describes
            which of the possible stream parameters you are interested in.
        """

        if params['_requesting'] == 'sources':
            return self._get_sources()

        if params['_requesting'] == 'protocols':
            return self._get_protocols()
        
        if params['_requesting'] == 'metrics':
            return self._get_metrics()

        return []

    def _get_sources(self):
        """ Get the names of all of the sources that have lpi flows data """
        return self.sources.keys()

    def _get_protocols(self):
        return self.protocols.keys()

    def _get_metrics(self):
        return self.metrics.keys()




# vim: set smartindent shiftwidth=4 tabstop=4 softtabstop=4 expandtab :
